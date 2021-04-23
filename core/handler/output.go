package handler

import (
	"context"
	"fmt"
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

func (o *Output) Write(records ...*model2.WalData) error {
	if len(records) == 0 {
		return nil
	}
	var client = proto.NewLogicalHandlerClient(o.connect)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	var res, err = client.Call(ctx, &proto.CallMessage{Table: "organization", Key: "id", Operate: records[0].OperationType.String()})
	if err != nil {
		fmt.Println(err, "123123123")
	}
	fmt.Println(res.Message)
	return nil
}

func (o *Output) Close() {
	_ = o.connect.Close()
}
