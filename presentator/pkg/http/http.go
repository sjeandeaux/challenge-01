package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	pb "github.com/sjeandeaux/presentator/presentator-grpc"
	"google.golang.org/grpc"
)

// RunServer runs the http server
func RunServer(ctx context.Context, portHTTP int, portGRPC int) (int, error) {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", portHTTP))
	if err != nil {
		return -1, err
	}

	ctx, cancel := context.WithCancel(ctx)
	go func(list net.Listener, ctx context.Context, cancel context.CancelFunc) {
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", portGRPC), grpc.WithInsecure())
		if err != nil {
			log.Println(err)
			cancel() //TODO manage error
		}
		defer conn.Close()
		client := pb.NewMatchingAggregatorServiceClient(conn)

		mux := http.NewServeMux()
		mux.HandleFunc("/presentator/v1/match-aggregator/request/", matchingAggregatorHandler(ctx, client.MatchingAggregatorRequest))
		mux.HandleFunc("/presentator/v1/match-aggregator/driver/", matchingAggregatorHandler(ctx, client.MatchingAggregatorDriver))

		s := &http.Server{
			ReadTimeout:  1 * time.Second,
			WriteTimeout: 10 * time.Second,
			Handler:      mux,
		}

		if err := s.Serve(lis); err != nil {
			log.Println(err) //TODO manage the error
			cancel()
		}

	}(lis, ctx, cancel)

	return lis.Addr().(*net.TCPAddr).Port, nil
}

func matchingAggregatorHandler(ctx context.Context, matchingAggregator func(context.Context, *pb.Request, ...grpc.CallOption) (*pb.Reply, error)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var request pb.Request
		if r.Body == nil {
			http.Error(w, "Please send a request body", 400)
			return
		}
		defer r.Body.Close()
		err := json.NewDecoder(r.Body).Decode(&request)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		value, err := matchingAggregator(ctx, &request)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		json.NewEncoder(w).Encode(value)
	}
}
