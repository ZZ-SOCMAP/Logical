package model

import (
	"sync"

	"logical/conf"
)

// WalData represent parsed wal log data
type WalData struct {
	OperationType Operation
	Schema        string
	Table         string
	Data          map[string]interface{}
	Timestamp     int64
	Pos           uint64
	Rule          *conf.Rule
}

// Reset for reuse
func (d *WalData) Reset() {
	d.OperationType = Unknow
	d.Schema = ""
	d.Table = ""
	d.Data = nil
	d.Timestamp = 0
	d.Pos = 0
	d.Rule = nil
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