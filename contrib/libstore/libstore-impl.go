// Your implementation of the libstore should go here.
package libstore

import (
	"P2-f12/official/lsplog"
	"P2-f12/official/storageproto"
  "net/rpc"
  "time"
)

type Libstore struct {
  Nodes []storageproto.Node
  RPCConn []*rpc.Client
}

func iNewLibstore(server, myhostport string, flags int) (*Libstore, error) {
  var store Libstore
  var master *rpc.Client
  var getargs storageproto.GetServersArgs
  var getreply storageproto.RegisterReply
  var err error

  master, err = rpc.DialHTTP("tcp", server)
  if lsplog.CheckReport(1, err) {
	  return nil, lsplog.NotImplemented("iNewLibstore")
  }

  master.Call("GetServers", &getargs, &getreply)

  for i := 0; (getreply.Ready == false) && (i < 5); i++ {
    time.Sleep(1000 * time.Millisecond)
    master.Call("GetServers", &getargs, &getreply)
  }

  // couldn't get list of servers from master
  if getreply.Ready == false {
	  return nil, lsplog.NotImplemented("iNewLibstore")
  }

  store.Nodes = getreply.Servers
  store.RPCConn = make([]*rpc.Client, len(store.Nodes))
  store.RPCConn[0] = master

	return &store, nil
}

func (ls *Libstore) iGet(key string) (string, error) {
	return "", lsplog.NotImplemented("iGet")
}

func (ls *Libstore) iPut(key, value string) error {
	return lsplog.NotImplemented("iPut")
}

func (ls *Libstore) iGetList(key string) ([]string, error) {
	return nil, lsplog.NotImplemented("iGetList")
}

func (ls *Libstore) iRemoveFromList(key, removeitem string) error {
	return lsplog.NotImplemented("iRemoveFromList")
}

func (ls *Libstore) iAppendToList(key, newitem string) error {
	return lsplog.NotImplemented("iAppendToList")
}
