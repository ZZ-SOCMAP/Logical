package handler

import (
	"context"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"logical/config"
	model2 "logical/core/model"
	"logical/proto"
	"time"
)

type Output struct {
	upstream *config.Upstream
	connect  *grpc.ClientConn
}

// NewOutput create new output
func NewOutput(upstream *config.Upstream, connect *grpc.ClientConn) Output {
	return Output{upstream: upstream, connect: connect}
}

// Write send a message to rpc handler
func (o *Output) Write(records ...*model2.WalData) error {
	var client = proto.NewLogicalHandlerClient(o.connect)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var message proto.CallMessage
	for i := 0; i < len(records); i++ {
		message.Table = records[i].Table
		message.Operate = records[i].OperationType.String()
		message.Id = records[i].Data["id"].(int64)
		if reply, err := client.Call(ctx, &message); err != nil {
			zap.L().Error(message.String(), zap.Error(err))
			return nil
		} else {
			zap.L().Info(message.String(), zap.String("reply", reply.String()))
		}
	}
	return nil
}

// Close of rpc connection
func (o *Output) Close() {
	_ = o.connect.Close()
}
