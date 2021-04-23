package handler

import (
	"context"
	model2 "logical/core/model"

	"logical/conf"
)

// Handler handle dump data
type Handler interface {
	// Handle row data
	Handle(wal ...*model2.WalData) error
	Stop()
}

// PosCallback for handler
type PosCallback func(uint64)

// NewHandler create wal handler with subscribe config
func NewHandler(sub *conf.SubscribeConfig, callback PosCallback) Handler {
	ret := &wrapper{
		dataCh:    make(chan []*model2.WalData, 20480),
		callback:  callback,
		rules:     sub.Tables,
		sub:       sub,
		ruleCache: map[string]string{},
		skipCache: map[string]struct{}{},
		done:      make(chan struct{}),
	}

	ret.output = NewOutput(sub.Upstream)
	ctx, cancel := context.WithCancel(context.Background())
	ret.cancel = cancel
	go ret.runloop(ctx)
	return ret
}
