package main


// A simple main that starts your tribble server.
// DO NOT MODIFY THIS FILE

import (
	"fmt"
	"flag"
	"net"
	"net/http"
	"net/rpc"
	"log"
	"P2-f12/official/tribimpl"
)



var portnum *int = flag.Int("port", 9010, "port # to listen on")

func main() {
	flag.Parse()
	if (flag.NArg() < 1) {
		log.Fatal("usage:  tribserver <storage master node>")
	}
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", *portnum))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Server starting on port %d\n", *portnum);
	ts := tribimpl.NewTribserver(flag.Arg(0), fmt.Sprintf("localhost:%d", *portnum))
	rpc.Register(ts)
	rpc.HandleHTTP()
	http.Serve(l, nil)
}
