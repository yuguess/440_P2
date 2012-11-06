/** @file libstore-impl.go
 *  @brief implementation of libstore 
 *  @author Andrin(atrejo) Dalong CHENG(dalongc)
 *  @date 2012-10-23
 */

package libstore

import (
  "fmt"
  "net"
  "net/rpc"
  "sort"
  "strings"
  "time"
  "P2-f12/contrib/cache"
  "P2-f12/official/cacherpc"
  "P2-f12/official/lsplog"
  "P2-f12/official/storageproto"
)

type NodeList []storageproto.Node

type Libstore struct {
  Nodes NodeList
  RPCConn []*rpc.Client

  LeaseConn net.Listener
  Addr string
  Flags int

  Leases *cache.Cache
}

var StatusName = map[int]string {
  storageproto.OK:            "OK",
  storageproto.EKEYNOTFOUND:  "KEYNOTFOUND",
  storageproto.EITEMNOTFOUND: "ITEMNOTFOUND",
  storageproto.EWRONGSERVER:  "WRONGSERVER",
  storageproto.EPUTFAILED:    "PUTFAILED",
  storageproto.EITEMEXISTS:   "ITEMEXISTS",
}

/**@brief helper function for sorting  
 * @param void 
 * @return int 
 */
func (list NodeList) Len() int {
  return len(list)
}

/**@brief helper function for sorting  
 * @param i 
 * @param j
 * @return void 
 */
func (list NodeList) Swap(i, j int) {
  list[j], list[i] = list[i], list[j]
}

/**@brief helper function for sorting  
 * @param i 
 * @param j
 * @return bool 
 */
func (list NodeList) Less(i, j int) bool {
  return list[i].NodeID < list[j].NodeID
}

/**@brief helper function for sorting  
 * @param server master storage server addr 
 * @param myhostport trib server's port  
 * @param flags 
 * @return *Libstore 
 * @return error
 */
func iNewLibstore(server, myhostport string, flags int) (*Libstore, error) {
  var store Libstore
  var master *rpc.Client
  var args storageproto.GetServersArgs
  var reply storageproto.RegisterReply
  var err error

  store.Addr = myhostport
  store.Flags = flags

  if store.Addr != "" {
    rpc.Register(cacherpc.NewCacheRPC(&store))
  }

  lsplog.Vlogf(3, "libstore try to connect to master storage %s", server)

  master, err = rpc.DialHTTP("tcp", server)
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  lsplog.Vlogf(3, "try to call GetServers")

  master.Call("StorageRPC.GetServers", &args, &reply)

  if !reply.Ready {
    for i := 0; (i < 5); i++ {
      time.Sleep(1000 * time.Millisecond)
      master.Call("StorageRPC.GetServers", &args, &reply)
    }
  }

  err = master.Close()
  if lsplog.CheckReport(1, err) {
    lsplog.Vlogf(3, "WARNING close master failed")
  }

  // couldn't get list of servers from master
  if (reply.Ready == false) || (reply.Servers == nil) {
    return nil, lsplog.MakeErr("Storage system not ready.")
  }

  store.Nodes = reply.Servers
  store.RPCConn = make([]*rpc.Client, len(store.Nodes))

  sort.Sort(store.Nodes)
  /*
  for i := 0; i < len(store.Nodes); i++ {
    fmt.Printf("%v\n", store.Nodes[i])
  }*/

  store.Leases = cache.NewCache()
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  lsplog.Vlogf(3, "libstore create complete")

  return &store, nil
}

/**@brief helper function for sorting  
 * @param function 
 * @param status  
 * @return error
 */
func MakeErr(function string, status int) lsplog.LspErr {
  var str string
  str = fmt.Sprintf("%s failed: %s (%d)", function, StatusName[status], status)

  return lsplog.MakeErr(str)
}

/**@brief Hashes a key and returns an RPC connection to the server 
          responsible for storing it. If an RPC connection is not 
          established, create one and store it for future accesses.  
 * @param server master server addr 
 * @param myhostport trib server's port  
 * @param flags 
 * @return *Libstore 
 * @return error
 */
func (ls *Libstore) GetServer(key string) (*rpc.Client, error) {
  var id uint32
  var svr int
  var err error

  //lsplog.Vlogf(3, "libstore GetServer Invoked")

  id = Storehash(strings.Split(key, ":")[0])

  // returns the index of the first server after the key's hash
  svr = sort.Search(
      len(ls.Nodes), func(i int) bool { return ls.Nodes[i].NodeID >= id })
  svr = (svr) % len(ls.Nodes)

  //lsplog.Vlogf(0, "%s -> %d (%d)\n", key, id, svr)

  if ls.RPCConn[svr] == nil {
    lsplog.Vlogf(0, "Caching RPC connection to %s.\n", ls.Nodes[svr].HostPort)
    ls.RPCConn[svr], err = rpc.DialHTTP("tcp", ls.Nodes[svr].HostPort)
    if lsplog.CheckReport(1, err) {
      return nil, err
    }
  }

  return ls.RPCConn[svr], nil
}

/**@brief Get value given a key for storage server  
 * @param key 
 * @return value 
 * @return error
 */
func (ls *Libstore) iGet(key string) (string, error) {
  var cli *rpc.Client
  var args storageproto.GetArgs = storageproto.GetArgs{key, false, ls.Addr}
  var reply storageproto.GetReply
  var err error

  //try cache first
  if tmp, err := ls.Leases.Get(key, &args); err == nil {
    reply.Value = tmp.(string)
    return reply.Value, nil
  }

  if (ls.Flags & ALWAYS_LEASE) != 0 {
    args.WantLease = true
  }

  //lsplog.Vlogf(0, "libstore Get %s\n", key)

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return "", err
  }

  //listen on no port to accept revoke
  if ls.Addr == "" {
    args.WantLease = false
  }

  //lsplog.Vlogf(0, "Get args:%v\n", args)

  err = cli.Call("StorageRPC.Get", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return "", err
  }

  //lsplog.Vlogf(0, "Get reply:%v#!!\n", reply)
  //fmt.Printf("Get reply granted:%v#!#\n", reply.Lease.Granted)

  if reply.Lease.Granted {
    ls.Leases.LeaseGranted(key, reply.Value, reply.Lease)
  }

  if reply.Status != storageproto.OK {
    return "", MakeErr("Get()", reply.Status)
  }

  return reply.Value, nil
}

/**@brief store key-value into backend 
 * @param key string 
 * @param value string 
 * @return error
 */
func (ls *Libstore) iPut(key, value string) error {
  var cli *rpc.Client
  var args storageproto.PutArgs = storageproto.PutArgs{key, value}
  var reply storageproto.PutReply
  var err error

  //lsplog.Vlogf(0, "libstore put %s->%s!", key, value)

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return err
  }

  //lsplog.Vlogf(0, "libstore getserver complete!")
  //lsplog.Vlogf(0, "put args %v\n", args)
  /*
  fmt.Printf("put args %v\n", args)
  fmt.Printf("here2\n")
  */

  err = cli.Call("StorageRPC.Put", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return err
  }

  //fmt.Printf("put reply %v\n", reply)
  //lsplog.Vlogf(0, "put reply %v\n", reply)

  if reply.Status != storageproto.OK {
    return MakeErr("Put()", reply.Status)
  }

  return nil
}

/**@brief given a key, get list of strings  
 * @param key 
 * @return string[] 
 * @return error
 */
func (ls *Libstore) iGetList(key string) ([]string, error) {
  var cli *rpc.Client
  var args storageproto.GetArgs = storageproto.GetArgs{key, false, ls.Addr}
  var reply storageproto.GetListReply
  var err error

  //try cache first
  if tmp, err := ls.Leases.Get(key, &args); err == nil {
    reply.Value = tmp.([]string)
    return reply.Value, nil
  }

  if (ls.Flags & ALWAYS_LEASE) != 0 {
    args.WantLease = true
  }

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  //lsplog.Vlogf(0, "GetList args %v", args)

  err = cli.Call("StorageRPC.GetList", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return nil, err
  }

  //lsplog.Vlogf(0, "GetList reply %v", reply)

  if reply.Lease.Granted {
    ls.Leases.LeaseGranted(key, reply.Value, reply.Lease)
  }

  if reply.Status != storageproto.OK {
    return nil, MakeErr("GetList()", reply.Status)
  }

  return reply.Value, nil
}

/**@brief remove a item from backend storage   
 * @param key 
 * @param removeitem 
 * @return error
 */
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

/**@brief append newitem to list  
 * @param key 
 * @param newitem 
 * @return error
 */
func (ls *Libstore) iAppendToList(key, newitem string) error {
  var cli *rpc.Client
  var args storageproto.PutArgs = storageproto.PutArgs{key, newitem}
  var reply storageproto.PutReply
  var err error

  cli, err = ls.GetServer(key)
  if lsplog.CheckReport(1, err) {
    return err
  }

  //lsplog.Vlogf(0, "AppendToList args %v\n", args)

  err = cli.Call("StorageRPC.AppendToList", &args, &reply)
  if lsplog.CheckReport(1, err) {
    return err
  }

  //lsplog.Vlogf(0, "AppendToList reply %v\n", reply)

  if reply.Status != storageproto.OK {
    return MakeErr("AppendToList()", reply.Status)
  }

  return nil
}

/**@brief revoke function called by storage server to invalidate  
 *        libstore cache entry
 * @param RevokeLeaseArgs
 * @param RevokeLeaseReply
 * @return error
 */
func (ls *Libstore) RevokeLease(
    args *storageproto.RevokeLeaseArgs,
    reply *storageproto.RevokeLeaseReply) error {


  var valid bool

  //fmt.Printf("libstore Revoking lease: %s\n", args.Key)

  valid = ls.Leases.ClearEntry(args.Key)
  if !valid {
    reply.Status = storageproto.EKEYNOTFOUND
  } else {
    reply.Status = storageproto.OK
  }

  return nil
}
