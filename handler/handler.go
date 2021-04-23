package handler

import (
	"context"

	"logical/conf"
	"logical/handler/output"
	"logical/model"
)

// Handler handle dump data
type Handler interface {
	// Handle row data
	Handle(wal ...*model.WalData) error
	Stop()
}

// PosCallback for handler
type PosCallback func(uint64)

// NewHandler create wal handler with subscribe config
func NewHandler(sub *conf.Subscribe, callback PosCallback) Handler {
	ret := &handlerWrapper{
		dataCh:    make(chan []*model.WalData, 20480),
		callback:  callback,
		rules:     sub.Rules,
		sub:       sub,
		ruleCache: map[string]string{},
		skipCache: map[string]struct{}{},
		done:      make(chan struct{}),
	}

	ret.output = output.NewOutput(sub)
	ctx, cancel := context.WithCancel(context.Background())
	ret.cancel = cancel
	go ret.runloop(ctx)
	return ret
}
