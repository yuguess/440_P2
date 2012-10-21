// Your implementation of the libstore should go here.
package libstore

import (
  "P2-f12/official/lsplog"
  "P2-f12/official/storageproto"
  "net/rpc"
  "strings"
  "time"
  "hash/fnv"
)

type Libstore struct {
  Nodes []storageproto.Node
  RPCConn []*rpc.Client
  Addr string
  Flags int
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
    master.Call("GetServers", &args, &reply)
  }

  // couldn't get list of servers from master
  if (reply.Ready == false) || (reply.Servers == nil) {
    return nil, lsplog.MakeErr("Storage system not ready.")
  }

  store.Nodes = reply.Servers
  store.RPCConn = make([]*rpc.Client, len(store.Nodes))
  store.RPCConn[0] = master

  return &store, nil
}

func Storehash(key string) uint32 {
  hasher := fnv.New32()
  hasher.Write([]byte(key))
  return hasher.Sum32()
}

// supports only one server
// TODO: return connection to server in consistent hash ring, opening
// connection if necessary
func (ls *Libstore) GetServer(key string) (*rpc.Client, error) {
  var shard string
  var id uint32

  shard = strings.Split(key, ":")[0]
  id = Storehash(shard)

  lsplog.Vlogf(4, "%s -> %d\n", key, id)

  return ls.RPCConn[0], nil
}

// TODO: return storageproto error to tribserver
func (ls *Libstore) iGet(key string) (string, error) {
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
    return "", lsplog.MakeErr("Get() failed.")
  }

  return reply.Value, nil
}

func (ls *Libstore) iPut(key, value string) error {
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

  if reply.Status != storageproto.OK {
    return lsplog.MakeErr("Put() failed.")
  }

  return nil
}

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

  if reply.Status != storageproto.OK {
    return nil, lsplog.MakeErr("GetList() failed.")
  }

  return reply.Value, nil
}

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

  if reply.Status != storageproto.OK {
    return lsplog.MakeErr("RemoveFromList() failed.")
  }

  return nil
}

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

  if reply.Status != storageproto.OK {
    return lsplog.MakeErr("AppendToList() failed.")
  }

  return nil
}
