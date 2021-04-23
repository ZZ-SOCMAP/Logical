package dump

import (
	"bufio"
	"fmt"
	"io"
	handler2 "logical/core/handler"
	model2 "logical/core/model"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

type parser struct {
	r io.Reader
}

func newParser(r io.Reader) *parser {
	return &parser{r: r}
}

func (p *parser) parse(h handler2.Handler) error {
	rb := bufio.NewReaderSize(p.r, 1024*16)
	for {
		var line, err = rb.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		data := p.parseSql(line)
		if data == nil {
			continue
		}
		if err = h.Handle(data); err != nil {
			return err
		}
	}
	return nil
}

func (p *parser) parseSql(line string) *model2.WalData {
	if !strings.HasPrefix(line, "INSERT") {
		return nil
	}
	stmt, err := sqlparser.Parse(line)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	switch row := stmt.(type) {
	case *sqlparser.Insert:
		var data = map[string]interface{}{}
		var columns []string
		for i := 0; i < len(row.Columns); i++ {
			columns = append(columns, row.Columns[i].String())
		}
		if columns == nil {
			return nil
		}
		if values, ok := row.Rows.(sqlparser.Values); ok {
			for i := 0; i < len(values[0]); i++ {
				switch val := values[0][i].(type) {
				case *sqlparser.SQLVal:
					data[columns[i]] = p.parseSQLVal(val)
				case *sqlparser.NullVal:
					data[columns[i]] = nil
				}
			}
			return &model2.WalData{OperationType: model2.Insert, Schema: row.Table.Qualifier.String(), Table: row.Table.Name.String(), Data: data}
		}
	}
	return nil
}

func (p *parser) parseSQLVal(val *sqlparser.SQLVal) interface{} {
	switch val.Type {
	case sqlparser.StrVal:
		return string(val.Val)
	case sqlparser.IntVal:
		ret, _ := strconv.ParseInt(string(val.Val), 10, 64)
		return ret
	case sqlparser.FloatVal:
		ret, _ := strconv.ParseFloat(string(val.Val), 64)
		return ret
	case sqlparser.HexNum:
		return string(val.Val)
	case sqlparser.HexVal:
		return string(val.Val)
	case sqlparser.ValArg:
		return string(val.Val)
	case sqlparser.BitVal:
		return string(val.Val)

	}
	return string(val.Val)
}
