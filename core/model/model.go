package model

import (
	"fmt"
	"github.com/jackc/pgx"
	jsoniter "github.com/json-iterator/go"
	"github.com/nickelser/parselogical"
	"logical/config"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

type Operate uint8

const (
	UNKNOW Operate = iota
	BEGIN
	INSERT
	DELETE
	UPDATE
	COMMIT
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var operate = sync.Map{}
var unoperate = [6]string{"UNKNOW", "BEGIN", "INSERT", "DELETE", "UPDATE", "COMMIT"}

func init() {
	operate.Store("INSERT", INSERT)
	operate.Store("UPDATE", UPDATE)
	operate.Store("DELETE", DELETE)
	operate.Store("BEGIN", BEGIN)
	operate.Store("COMMIT", COMMIT)
	operate.Store("UNKNOW", UNKNOW)
}

func (o Operate) String() string {
	return unoperate[int(o)]
}

// WalData represent parsed wal logger data
type WalData struct {
	OperationType Operate
	Schema        string
	Table         string
	Data          map[string]interface{}
	Timestamp     int64
	Pos           uint64
	Rule          string
}

// Reset for reuse
func (w *WalData) Reset() {
	w.OperationType = UNKNOW
	w.Schema = ""
	w.Table = ""
	w.Data = nil
	w.Timestamp = 0
	w.Pos = 0
	w.Rule = ""
}

// reusing data objects, reduce the pressure of Garbage Collection
var pool = sync.Pool{New: func() interface{} { return &WalData{} }}

func NewData() *WalData {
	data := pool.Get().(*WalData)
	data.Reset()
	return data
}

func PutData(data *WalData) {
	pool.Put(data)
}

func (w *WalData) Decode(wal *pgx.WalMessage, tableCfg *config.CaptureTableConfig) error {
	result := parselogical.NewParseResult(*(*string)(unsafe.Pointer(&wal.WalData)))
	if err := result.Parse(); err != nil {
		return err
	}
	var schema, table string
	if result.Relation != "" {
		i := strings.IndexByte(result.Relation, '.')
		if i < 0 {
			table = result.Relation
		} else {
			schema = result.Relation[:i]
			table = strings.ReplaceAll(result.Relation[i+1:], `"`, "")
		}
		// table name based filtering
		if table != tableCfg.Name {
			return nil
		}
		w.Schema = schema
		w.Table = table
	}
	w.Pos = wal.WalStart
	op, ok := operate.Load(result.Operation)
	if !ok {
		return nil
	}
	w.OperationType = op.(Operate)
	w.Timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	if len(result.Columns) == 0 {
		return nil
	}
	w.Data = make(map[string]interface{}, len(result.Columns))
	for key, cell := range result.Columns {
		if _, ok = tableCfg.Rule[key]; !ok {
			continue
		}
		var value interface{}
		if cell.Value != "null" {
			switch cell.Type {
			case "boolean":
				value, _ = strconv.ParseBool(cell.Value)
			case "smallint", "integer", "bigint", "smallserial", "serial", "bigserial", "interval":
				value, _ = strconv.ParseInt(cell.Value, 10, 64)
			case "float", "decimal", "numeric", "double precision", "real":
				value, _ = strconv.ParseFloat(cell.Value, 64)
			case "character varying[]":
				value = strings.Split(cell.Value[1:len(cell.Value)-1], ",")
			case "jsonb":
				value = make(map[string]interface{})
				_ = json.UnmarshalFromString(cell.Value, &value)
			case "timestamp without time zone":
				value, _ = time.Parse(config.TimeLayout, cell.Value)
			default:
				value = cell.Value
			}
		}
		w.Data[key] = value
	}
	w.Data["pk"] = fmt.Sprintf("%v", w.Data[tableCfg.Pk])
	w.Data["operate"] = w.OperationType.String()
	return nil
}
