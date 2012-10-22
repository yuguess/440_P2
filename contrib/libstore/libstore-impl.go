// Your implementation of the libstore should go here.
package libstore

import (
  "fmt"
  "net"
  "net/http"
  "net/rpc"
  "sort"
  "strings"
  "time"
  "P2-f12/official/lsplog"
  "P2-f12/official/storageproto"
  "P2-f12/official/cacherpc"
  "P2-f12/contrib/cache"
)

type NodeList []storageproto.Node
/*
type KeyInfo struct {
  //queryTime time.Time
  
  FirstQuery time.Time
  NumQueries int
  Granted bool
  Duration int
  Data interface{}
  
}*/

type Libstore struct {
  Nodes NodeList
  RPCConn []*rpc.Client

  LeaseConn net.Listener
  Addr string
  Flags int

  localCache *cache.Cache
  //Leases map [string] KeyInfo
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

  if store.Addr != "" {
    store.LeaseConn, err = net.Listen("tcp", store.Addr)
    if lsplog.CheckReport(1, err) {
      return nil, err
    }

    rpc.Register(cacherpc.NewCacheRPC(&store))
    rpc.HandleHTTP()
    go http.Serve(store.LeaseConn, nil)
  }

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

  store.localCache, err = cache.NewCache()
  if lsplog.CheckReport(1, err) {
    return nil, err
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
  var cli *rpc.Client
  var args storageproto.GetArgs = storageproto.GetArgs{key, false, ls.Addr}
  var reply storageproto.GetReply
  var err error

/*
  if (ls.Flags & ALWAYS_LEASE) != 0 {
    args.WantLease = true
  }
*/

  //try cache first
  if tmp, err := ls.localCache.Get(key, &args); err == nil {
    reply.Value = tmp.(string)
    return reply.Value, nil
  }

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
  var cli *rpc.Client
  var args storageproto.PutArgs = storageproto.PutArgs{key, value}
  var reply storageproto.PutReply
  var err error

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return err
  }

  err = cli.Call("StorageRPC.Put", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return err
  }

  if reply.Status != storageproto.OK {
    return MakeErr("Put()", reply.Status)
  }

  return nil
}

func (ls *Libstore) iGetList(key string) ([]string, error) {
  var cli *rpc.Client
  var args storageproto.GetArgs = storageproto.GetArgs{key, false, ls.Addr}
  var reply storageproto.GetListReply
  var err error

  //try cache first
  if tmp, err := ls.localCache.Get(key, &args); err == nil {
    reply.Value = tmp.([]string)
    return reply.Value, nil
  }
/*
  if (ls.Flags & ALWAYS_LEASE) != 0 {
    args.WantLease = true
  }
*/
  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  err = cli.Call("StorageRPC.GetList", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  if reply.Status != storageproto.OK {
    return nil, MakeErr("GetList()", reply.Status)
  }

  return reply.Value, nil
}

func (ls *Libstore) iRemoveFromList(key, removeitem string) error {
  var cli *rpc.Client
  var args storageproto.PutArgs = storageproto.PutArgs{key, removeitem}
  var reply storageproto.PutReply
  var err error

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return err
  }

  err = cli.Call("StorageRPC.RemoveFromList", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return err
  }

  if reply.Status != storageproto.OK {
    return MakeErr("RemoveFromList()", reply.Status)
  }

  return nil
}

func (ls *Libstore) iAppendToList(key, newitem string) error {
  var cli *rpc.Client
  var args storageproto.PutArgs = storageproto.PutArgs{key, newitem}
  var reply storageproto.PutReply
  var err error

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return err
  }

  err = cli.Call("StorageRPC.AppendToList", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return err
  }

  if reply.Status != storageproto.OK {
    return MakeErr("AppendToList()", reply.Status)
  }

  return nil
}

func (ls *Libstore) RevokeLease(args *storageproto.RevokeLeaseArgs,
                              reply *storageproto.RevokeLeaseReply) error {
  err := ls.localCache.ClearEntry(args.Key)
  if err != nil {
    reply.Status = storageproto.EKEYNOTFOUND
    return nil
  }

  reply.Status = storageproto.OK
  return nil
}
