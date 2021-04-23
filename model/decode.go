
package model

import (
	"strconv"
	"strings"

	"logical/util"
	"github.com/jackc/pgx"
	"github.com/nickelser/parselogical"
)

// Parse test_decoding format wal to WalData
func Parse(msg *pgx.WalMessage) (*WalData, error) {
	result := parselogical.NewParseResult(util.Bytes2String(msg.WalData))
	if err := result.Parse(); err != nil {
		return nil, err
	}
	var ret = NewWalData()

	var schema, table string
	if result.Relation != "" {
		i := strings.IndexByte(result.Relation, '.')
		if i < 0 {
			table = result.Relation
		} else {
			schema = result.Relation[:i]
			table = result.Relation[i+1:]
		}

		ret.Schema = schema
		ret.Table = table
	}
	ret.Pos = msg.WalStart
	switch result.Operation {
	case "INSERT":
		ret.OperationType = Insert
	case "UPDATE":
		ret.OperationType = Update
	case "DELETE":
		ret.OperationType = Delete
	case "BEGIN":
		ret.OperationType = Begin
	case "COMMIT":
		ret.OperationType = Commit
	}

	if len(result.Columns) > 0 {
		ret.Data = make(map[string]interface{}, len(result.Columns))
	}
	for key, column := range result.Columns {
		if column.Quoted {
			ret.Data[key] = column.Value
			continue
		}

		if column.Value == "null" {
			ret.Data[key] = nil
			continue
		}

		if val, err := strconv.ParseInt(column.Value, 10, 64); err == nil {
			ret.Data[key] = val
			continue
		}
		if val, err := strconv.ParseFloat(column.Value, 64); err == nil {
			ret.Data[key] = val
			continue
		}
		ret.Data[key] = column.Value
	}

	return ret, nil
}
