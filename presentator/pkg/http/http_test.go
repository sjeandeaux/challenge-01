package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/sjeandeaux/presentator/pkg/grpc"
	pb "github.com/sjeandeaux/presentator/presentator-grpc"
	"github.com/stretchr/testify/assert"
)

func TestRunHTTPServerOK(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p, err := grpc.RunServer(ctx, 0, &matchingAggregatorServiceServer{reply: &pb.Reply{Result: 666}})
	if err != nil {
		t.Fatal(err)
	}
	pHTTP, err := RunServer(ctx, 0, p)
	if err != nil {
		t.Fatal(err)
	}
	requests := []string{"http://localhost:%d/presentator/v1/match-aggregator/request/",
		"http://localhost:%d/presentator/v1/match-aggregator/driver/"}

	for _, request := range requests {
		resp, err := http.Post(fmt.Sprintf(request, pHTTP), "application/json", bytes.NewBuffer([]byte("{}")))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		assert.Equal(t, `{"result":666}`, string(body[:len(body)-1]))
	}
}

func TestRunHTTPServerKO(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p, err := grpc.RunServer(ctx, 0, &matchingAggregatorServiceServer{reply: nil, err: errors.New("Houston, we have a problem")})
	if err != nil {
		t.Fatal(err)
	}

	pHTTP, err := RunServer(ctx, 0, p)
	if err != nil {
		t.Fatal(err)
	}
	requests := []string{"http://localhost:%d/presentator/v1/match-aggregator/request/",
		"http://localhost:%d/presentator/v1/match-aggregator/driver/"}

	for _, request := range requests {
		resp, err := http.Post(fmt.Sprintf(request, pHTTP), "application/json", bytes.NewBuffer([]byte("{}")))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		assert.Equal(t, `rpc error: code = Unknown desc = Houston, we have a problem`, string(body[:len(body)-1]))
		assert.Equal(t, 500, resp.StatusCode)
	}
}

type matchingAggregatorServiceServer struct {
	reply *pb.Reply
	err   error
}

//MatchingAggregatorRequest the aggregation
func (m *matchingAggregatorServiceServer) MatchingAggregatorRequest(context.Context, *pb.Request) (*pb.Reply, error) {
	return m.reply, m.err
}

//MatchingAggregatorDriver the aggregation
func (m *matchingAggregatorServiceServer) MatchingAggregatorDriver(context.Context, *pb.Request) (*pb.Reply, error) {
	return m.reply, m.err
}
