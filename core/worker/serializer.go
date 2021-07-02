package worker

import (
	"bufio"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/xwb1989/sqlparser"
	"go.uber.org/zap"
	"io"
	"logical/core/model"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// database change data capture worker
type serializer struct {
	cfg   pgx.ConnConfig
	table string
	dump  string
}

// NewSerializer create a dumper
func NewSerializer(cfg pgx.ConnConfig, table, dump string) *serializer {
	if dump == "" {
		dump = "pg_dump"
	}
	dump, err := exec.LookPath(dump)
	if err != nil {
		return nil
	}
	return &serializer{cfg: cfg, table: table, dump: dump}
}

// Listen database snapshot, parse sql then write to handler
func (c *serializer) Listen(ssid string, recovery func(records []*model.WalData)) error {
	args := make([]string, 0, 16)
	args = append(args, fmt.Sprintf("--host=%s", c.cfg.Host))
	args = append(args, fmt.Sprintf("--port=%d", c.cfg.Port))
	args = append(args, fmt.Sprintf("--username=%s", c.cfg.User))
	args = append(args, fmt.Sprintf("--snapshot=%s", ssid))
	args = append(args, fmt.Sprintf(`--table=%s`, c.table))
	args = append(args, "--data-only")
	args = append(args, "--column-inserts")
	cmd := exec.Command(c.dump, args...)
	cmd.Env = append(cmd.Env, fmt.Sprintf("PGDATABASE=%s", c.cfg.Database))
	cmd.Env = append(cmd.Env, fmt.Sprintf("PGPASSWORD=%s", c.cfg.Password))
	var r, w = io.Pipe()
	cmd.Stdout = w
	cmd.Stderr = os.Stderr
	go func() { recovery(c.reader(r)) }()
	return w.CloseWithError(cmd.Run())
}

// reader sql log message
func (c *serializer) reader(r io.Reader) (records []*model.WalData) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		if data := c.format(scanner.Text()); data != nil {
			records = append(records, data)
		}
	}
	return records
}

// format sql to data
func (c *serializer) format(sql string) *model.WalData {
	zap.L().Debug(sql)
	if !strings.HasPrefix(sql, "INSERT") {
		return nil
	}
	statment, err := sqlparser.Parse(sql)
	if err != nil {
		zap.L().Error("parse sql error", zap.String("sql", sql), zap.Error(err))
		return nil
	}
	switch row := statment.(type) {
	case *sqlparser.Insert:
		data := map[string]interface{}{}
		if values, ok := row.Rows.(sqlparser.Values); ok {
			for i := 0; i < len(values[0]); i++ {
				column := row.Columns[i].String()
				switch value := values[0][i].(type) {
				case *sqlparser.SQLVal:
					switch value.Type {
					case sqlparser.IntVal:
						data[column], _ = strconv.ParseInt(string(value.Val), 10, 64)
					case sqlparser.FloatVal:
						data[column], _ = strconv.ParseFloat(string(value.Val), 64)
					default:
						data[column] = string(value.Val)
					}
				case *sqlparser.NullVal:
					data[column] = nil
				}
			}
			return &model.WalData{
				Data:          data,
				OperationType: model.INSERT,
				Schema:        row.Table.Qualifier.String(),
				Table:         row.Table.Name.String(),
			}
		}
	}
	return nil
}
