package dump

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"logical/handler"
	"logical/model"

	"github.com/xwb1989/sqlparser"
)

type parser struct {
	r io.Reader
}

func newParser(r io.Reader) *parser {
	return &parser{r: r}
}

func (p *parser) parse(h handler.Handler) error {
	rb := bufio.NewReaderSize(p.r, 1024*16)
	for {

		line, err := rb.ReadString('\n')
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
		if err := h.Handle(data); err != nil {
			return err
		}

	}
	return nil
}

func (p *parser) parseSql(line string) *model.WalData {
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
		for _, clm := range row.Columns {
			columns = append(columns, clm.String())
		}
		if values, ok := row.Rows.(sqlparser.Values); ok {

			value := values[0]
			for i, col := range value {
				name := columns[i]
				switch val := col.(type) {
				case *sqlparser.SQLVal:
					data[name] = p.parseSQLVal(val)
				case *sqlparser.NullVal:
					data[name] = nil
				}
			}
			return &model.WalData{OperationType: model.Insert, Schema: row.Table.Qualifier.String(), Table: row.Table.Name.String(), Data: data}
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
