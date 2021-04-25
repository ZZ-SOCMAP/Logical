package main

import (
	"context"
	"golang/proto"
	"google.golang.org/grpc"
	"log"
	"net"
)

const (
	port = ":50049"
)

type server struct {
	proto.UnimplementedLogicalHandlerServer
}

func (s *server) Ping(_ context.Context, _ *proto.PingMessage) (*proto.Reply, error) {
	log.Println("Received: ping")
	return &proto.Reply{Status: true, Message: "pong"}, nil
}

func (s *server) Call(_ context.Context, msg *proto.CallMessage) (*proto.Reply, error) {
	log.Printf("Received: %s", msg.String())
	return &proto.Reply{Status: true, Message: "success"}, nil
}

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterLogicalHandlerServer(s, &server{})
	if err = s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
