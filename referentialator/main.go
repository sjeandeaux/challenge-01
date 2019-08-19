package main

import (
	"flag"
	"fmt"
	"net/http"
)

type commandLine struct {
	portHTTP int
}

var cmdLine = &commandLine{}

func init() {
	flag.IntVar(&cmdLine.portHTTP, "http-port", 9090, "The http port")
	flag.Parse()
}

func main() {
	http.HandleFunc("/referentialator/v1/referentials/polygons.psv", polygons)
	http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", cmdLine.portHTTP), nil)
}

func polygons(res http.ResponseWriter, req *http.Request) {
	http.ServeFile(res, req, "./referentials/polygons.psv")
}
