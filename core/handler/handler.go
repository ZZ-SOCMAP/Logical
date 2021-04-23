package handler

import (
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"logical/core/model"
	"time"

	"logical/config"
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
func NewHandler(capture *config.Capture, upstream *config.Upstream, callback PosCallback) Handler {
	var w = &wrapper{
		dataCh:   make(chan []*model.WalData, 20480),
		callback: callback,
		//tables:    capture.Tables,
		capture:   capture,
		ruleCache: map[string]string{},
		skipCache: map[string]struct{}{},
		done:      make(chan struct{}),
	}
	var kacp = keepalive.ClientParameters{
		Time:                10 * time.Second, // 如果没有活动，则每10秒发送ping
		Timeout:             time.Second,      // ping超时1秒后断开连接
		PermitWithoutStream: true,             // 没有活动时发送ping
	}
	conn, err := grpc.Dial(upstream.Host, grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
	if err != nil {
		zap.L().Error("did not connect", zap.Error(err))
	}
	w.output = NewOutput(upstream, conn)
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	go w.runloop(ctx)
	return w
}
