package main

import (
	"P2-f12/contrib/storageimpl" // 'official' vs 'contrib' here
	"P2-f12/official/storagerpc"
	"log"
	"net"
  "fmt"
	"net/http"
	"net/rpc"
)

func main() {
  var masterPort string = "127.0.0.1:9009"
  var numNodes int = 2
  var portnum int = 9011
  var nodeID uint32 = 1

  //create slave node
  l, e := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", portnum))
  if e != nil {
		log.Fatal("listen error:", e)
	}

	ss := storageimpl.NewStorageserver(masterPort, numNodes, portnum, nodeID)
	srpc := storagerpc.NewStorageRPC(ss)
	rpc.Register(srpc)
	rpc.HandleHTTP()
	http.Serve(l, nil)
}
