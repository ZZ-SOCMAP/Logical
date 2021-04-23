package model

import (
	"sync"
)

// WalData represent parsed wal logger data
type WalData struct {
	OperationType Operation
	Schema        string
	Table         string
	Data          map[string]interface{}
	Timestamp     int64
	Pos           uint64
	Rule          string
}

// Reset for reuse
func (d *WalData) Reset() {
	d.OperationType = Unknow
	d.Schema = ""
	d.Table = ""
	d.Data = nil
	d.Timestamp = 0
	d.Pos = 0
	d.Rule = ""
}

var waldatapool = sync.Pool{New: func() interface{} { return &WalData{} }}

// NewWalData get data from pool
func NewWalData() *WalData {
	data := waldatapool.Get().(*WalData)
	data.Reset()
	return data
}

// PutWalData putback data to pool
func PutWalData(data *WalData) {
	waldatapool.Put(data)
}
