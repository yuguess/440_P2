// Your implementation of the libstore should go here.
package libstore

import (
  "fmt"
  "net/rpc"
  "sort"
  "strings"
  "time"
  "P2-f12/official/lsplog"
  "P2-f12/official/storageproto"
)

type NodeList []storageproto.Node

type Libstore struct {
/*
  store_clnt *rpc.Client
}

func iNewLibstore(server, myhostport string, flags int) (*Libstore, error) {
  lsplog.SetVerbose(3)
  //store_clnt, err := rpc.DialHTTP("tcp", net.JoinHostPort(server, myhostport))
  store_clnt, err := rpc.DialHTTP("tcp", net.JoinHostPort("localhost", "9009"))
	if err != nil {
    lsplog.CheckReport(1, err)
    return nil, lsplog.MakeErr("libstore can not connect to storage server")
	}

  libstore_server := &Libstore{store_clnt}
  return libstore_server, nil
	//return nil, lsplog.NotImplemented("iNewLibstore")
*/
  Nodes NodeList
  RPCConn []*rpc.Client
  Addr string
  Flags int
}

func (list NodeList) Len() int {
  return len(list)
}

func (list NodeList) Swap(i, j int) {
  list[j], list[i] = list[i], list[j]
}

func (list NodeList) Less(i, j int) bool {
  return list[i].NodeID < list[j].NodeID
}

func iNewLibstore(server, myhostport string, flags int) (*Libstore, error) {
  var store Libstore
  var master *rpc.Client
  var args storageproto.GetServersArgs
  var reply storageproto.RegisterReply
  var err error

  store.Addr = myhostport
  store.Flags = flags

  master, err = rpc.DialHTTP("tcp", server)
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  master.Call("StorageRPC.GetServers", &args, &reply)

  for i := 0; (reply.Ready == false) && (i < 5); i++ {
    time.Sleep(1000 * time.Millisecond)
    master.Call("StorageRPC.GetServers", &args, &reply)
  }

  // couldn't get list of servers from master
  if (reply.Ready == false) || (reply.Servers == nil) {
    return nil, lsplog.MakeErr("Storage system not ready.")
  }

  store.Nodes = reply.Servers
  store.RPCConn = make([]*rpc.Client, len(store.Nodes))

  sort.Sort(store.Nodes)
  for i := 0; i < len(store.Nodes); i++ {
    fmt.Printf("%v\n", store.Nodes[i])
  }

  return &store, nil
}

var StatusName = map[int]string {
  storageproto.OK:            "OK",
  storageproto.EKEYNOTFOUND:  "KEYNOTFOUND",
  storageproto.EITEMNOTFOUND: "ITEMNOTFOUND",
  storageproto.EWRONGSERVER:  "WRONGSERVER",
  storageproto.EPUTFAILED:    "PUTFAILED",
  storageproto.EITEMEXISTS:   "ITEMEXISTS",
}

func MakeErr(function string, status int) lsplog.LspErr {
  var str string
  str = fmt.Sprintf("%s failed: %s (%d)", function, StatusName[status], status)

  return lsplog.MakeErr(str)
}

/**
 * Hashes a key and returns an RPC connection to the server responsible for
 * storing it. If an RPC connection is not established, create one and store it
 * for future accesses.
 * */
func (ls *Libstore) GetServer(key string) (*rpc.Client, error) {
  var id uint32
  var svr int
  var err error

  id = Storehash(strings.Split(key, ":")[0])

  // returns the index of the first server after the key's hash
  svr = sort.Search(
      len(ls.Nodes), func(i int) bool { return ls.Nodes[i].NodeID > id })
  svr %= len(ls.Nodes)

  fmt.Printf("%s -> %d (%d)\n", key, id, svr)

  if ls.RPCConn[svr] == nil {
    fmt.Printf("Caching RPC connection to %s.\n", ls.Nodes[svr].HostPort)

    ls.RPCConn[svr], err = rpc.DialHTTP("tcp", ls.Nodes[svr].HostPort)
    if lsplog.CheckReport(1, err) {
      return nil, err
    }
  }

  return ls.RPCConn[svr], nil
}

// TODO: return storageproto error to tribserver
func (ls *Libstore) iGet(key string) (string, error) {
  /*
  args := &storageproto.GetArgs{key, false, ""}
  reply := &storageproto.GetReply{}

  ls.store_clnt.Call("StorageRPC.Get", args, reply)

  if reply.Status == storageproto.EKEYNOTFOUND {
    return  "", lsplog.MakeErr("Get key not found")
  */
  var args storageproto.GetArgs
  var reply storageproto.GetReply
  var err error
  var cli *rpc.Client

  args.Key = key
  args.WantLease = false
  args.LeaseClient = ls.Addr

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return "", err
  }

  err = cli.Call("StorageRPC.Get", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return "", err
  }

  if reply.Status != storageproto.OK {
    return "", MakeErr("Get()", reply.Status)
  }

  return reply.Value, nil
}

func (ls *Libstore) iPut(key, value string) error {
/*
  args := &storageproto.PutArgs{key, value}
  reply := &storageproto.PutReply{}

  ls.store_clnt.Call("StorageRPC.Put", args, reply)

  if reply.Status == storageproto.EITEMEXISTS {
    return  lsplog.MakeErr("Duplicate Put !")
  }

	return nil

	//return lsplog.NotImplemented("iPut")
}
  */
  var args storageproto.PutArgs = storageproto.PutArgs{key, value}
  var reply storageproto.PutReply
  var cli *rpc.Client
  var err error

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return err
  }

  err = cli.Call("StorageRPC.Put", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return err
  }

  if reply.Status == storageproto.EITEMEXISTS {
    return  lsplog.MakeErr("Duplicate Put !")
  }

  /*
  if reply.Status != storageproto.OK {
    return MakeErr("Put()", reply.Status)
  }*/

  return nil
}
/*
func (ls *Libstore) iGetList(key string) ([]string, error) {
  args := &storageproto.GetArgs{key, false, ""}
  reply := &storageproto.GetListReply{}

  ls.store_clnt.Call("StorageRPC.GetList", args, reply)

  if reply.Status == storageproto.EKEYNOTFOUND {
    return  nil, lsplog.MakeErr("GetList key not found")
  }

	return reply.Value, nil
}*/

func (ls *Libstore) iGetList(key string) ([]string, error) {
  var args storageproto.GetArgs = storageproto.GetArgs{key, false, ls.Addr}
  var reply storageproto.GetListReply
  var cli *rpc.Client
  var err error

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  err = cli.Call("StorageRPC.GetList", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  if reply.Status == storageproto.EKEYNOTFOUND {
    return  nil, lsplog.MakeErr("GetList key not found")
  }

/*
  if reply.Status != storageproto.OK {
    return nil, MakeErr("GetList()", reply.Status)
  }
*/
  return reply.Value, nil
}
/*
func (ls *Libstore) iRemoveFromList(key, removeitem string) error {
  args := &storageproto.PutArgs{key, removeitem}
  reply := &storageproto.PutReply{}

  ls.store_clnt.Call("StorageRPC.RemoveFromList", args, reply)

  if reply.Status == storageproto.EKEYNOTFOUND {
    return lsplog.MakeErr("RemoveList key not found")
  }

  if reply.Status == storageproto.EITEMNOTFOUND {
    return lsplog.MakeErr("Remove item not found in list")
  }

	return nil
}*/
func (ls *Libstore) iRemoveFromList(key, removeitem string) error {
  var args storageproto.PutArgs = storageproto.PutArgs{key, removeitem}
  var reply storageproto.PutReply
  var cli *rpc.Client
  var err error

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return err
  }

  err = cli.Call("StorageRPC.RemoveFromList", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return err
  }

  if reply.Status == storageproto.EKEYNOTFOUND {
    return lsplog.MakeErr("RemoveList key not found")
  }

  if reply.Status == storageproto.EITEMNOTFOUND {
    return lsplog.MakeErr("Remove item not found in list")
  }
  /*
  if reply.Status != storageproto.OK {
    return MakeErr("RemoveFromList()", reply.Status)
  }*/

  return nil
}
/*
func (ls *Libstore) iAppendToList(key, newitem string) error {
  args := &storageproto.PutArgs{key, newitem}
  reply := &storageproto.PutReply{}

  ls.store_clnt.Call("StorageRPC.AppendToList", args, reply)

  if reply.Status == storageproto.EKEYNOTFOUND {
    return lsplog.MakeErr("AppendList key not found")
  }

  if reply.Status == storageproto.EITEMEXISTS {
    return lsplog.MakeErr("Insert duplicate item to list")
  }

	return nil
} */

func (ls *Libstore) iAppendToList(key, newitem string) error {
  var args storageproto.PutArgs = storageproto.PutArgs{key, newitem}
  var reply storageproto.PutReply
  var cli *rpc.Client
  var err error

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return err
  }

  err = cli.Call("StorageRPC.AppendToList", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return err
  }
  if reply.Status == storageproto.EKEYNOTFOUND {
    return lsplog.MakeErr("AppendList key not found")
  }

  if reply.Status == storageproto.EITEMEXISTS {
    return lsplog.MakeErr("Insert duplicate item to list")
  }
  /*
  if reply.Status != storageproto.OK {
    return MakeErr("AppendToList()", reply.Status)
  }*/

  return nil
}
