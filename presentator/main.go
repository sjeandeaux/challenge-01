package main

import (
	"context"
	"flag"
	"log"

	"github.com/sjeandeaux/presentator/pkg/grpc"
	"github.com/sjeandeaux/presentator/pkg/http"
	"github.com/sjeandeaux/presentator/pkg/matchator"
)

type commandLine struct {
	matchatorConfig matchator.Config
	portGRPC        int
	portHTTP        int
}

var cmdLine = &commandLine{matchatorConfig: matchator.Config{}}

func init() {
	flag.StringVar(&cmdLine.matchatorConfig.Host, "mongo-host", "mongo", "The mongo host")
	flag.StringVar(&cmdLine.matchatorConfig.Login, "mongo-login", "devroot", "The mongo login")
	flag.StringVar(&cmdLine.matchatorConfig.Password, "mongo-password", "devroot", "The mongo password (it should a secret but out of laziness...")
	flag.IntVar(&cmdLine.matchatorConfig.Port, "mongo-port", 27017, "The mongo port")

	flag.StringVar(&cmdLine.matchatorConfig.Database, "mongo-database", "challenge", "The database")
	flag.StringVar(&cmdLine.matchatorConfig.RequestCollection, "mongo-request-collection", "requestRate", "The request collection")
	flag.StringVar(&cmdLine.matchatorConfig.DriverCollection, "mongo-driver-collection", "driverRate", "The driver collection")

	flag.IntVar(&cmdLine.portGRPC, "grpc-port", 8080, "The grpc port")
	flag.IntVar(&cmdLine.portHTTP, "http-port", 8686, "The http port")
	flag.Parse()
}

func main() {
	ctx := context.Background()

	matcho, err := matchator.NewMatchingAggregatorServiceServer(ctx, cmdLine.matchatorConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer matcho.Close()

	pGRP, err := grpc.RunServer(ctx, cmdLine.portGRPC, matcho)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Server GRPC on %d\n", pGRP)

	pHTTP, err := http.RunServer(ctx, cmdLine.portHTTP, pGRP)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Server HTTP on %d\n", pHTTP)
	<-ctx.Done()
}
