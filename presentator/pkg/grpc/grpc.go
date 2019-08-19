package grpc

import (
	"context"
	"fmt"
	"log"
	"net"


	"google.golang.org/grpc"

	pb "github.com/sjeandeaux/presentator/presentator-grpc"


)

// RunServer runs the grpc server
func RunServer(ctx context.Context, port int, matchingAggregatorServiceServer pb.MatchingAggregatorServiceServer) (int, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		return -1, err
	}

	ctx, cancel := context.WithCancel(ctx)
	go func(lis net.Listener, ctx context.Context, cancel context.CancelFunc) {
		grpcServer := grpc.NewServer()
		pb.RegisterMatchingAggregatorServiceServer(grpcServer, matchingAggregatorServiceServer)

		if err := grpcServer.Serve(lis); err != nil {
			log.Println(err) //TODO manage the error
			cancel()
		}
	}(lis, ctx, cancel)
	return lis.Addr().(*net.TCPAddr).Port,  nil
}
