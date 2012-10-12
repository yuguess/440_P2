/** @file tribserver.go
 *  @brief implement tribble server 
 *  @author Adrian() Dalong CHENG (dalongc)
 *  @date 2012-10-11
 */
package main

import (
	"fmt"
	"flag"
	"net"
	"net/http"
	"net/rpc"
	"log"
	"P2-f12/contrib/tribimpl"
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
	ts := tribimpl.NewTribserver(flag.Arg(0),
                            fmt.Sprintf("localhost:%d", *portnum))
	rpc.Register(ts)
	rpc.HandleHTTP()
	http.Serve(l, nil)
}
