package storageimpl

// The internal implementation of your storage server.
// Note:  This file does *not* provide a 'main' interface.  It is used
// by the 'storageserver' main function we have provided you, which
// will call storageimpl.NewStorageserver(...).
//
// Must implemement NewStorageserver and the RPC-able functions
// defined in the storagerpc StorageInterface interface.


import (
  //"bytes"
  "fmt"
  "net/rpc"
  "time"
  "P2-f12/official/storageproto"
  "P2-f12/official/lsplog"
  "encoding/json"
  "sync"
  //"P2-f12/official/tribproto"
  //"math"
  //"math/big"
  //"math/rand"
)

const DEFAULT_MASTER_PORT = 9009

type Storageserver struct {
  hash map[string] []byte
  portnum int
  nodeid uint32
  isMaster bool
  nodes map[storageproto.Node] bool
  numnodes int
  rwlock sync.RWMutex
}

func reallySeedTheDamnRNG() {
	//randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	//rand.Seed( randint.Int64())
}

func NewStorageserver(master string, numnodes int, portnum int,
                                        nodeid uint32) *Storageserver {

  lsplog.SetVerbose(3)
  lsplog.Vlogf(3, "Create New Storage Server")
  lsplog.Vlogf(3, "master:%s, numnodes:%d, portnum:%d, nodeid:%d",
                                      master, numnodes, portnum, nodeid)
  var masterNode *rpc.Client
  var regArgs storageproto.RegisterArgs
  var regReply storageproto.RegisterReply
  var storage Storageserver
  var err error
  var nodes = make(map[storageproto.Node] bool)

  storage.nodeid = nodeid

  if master == "" {
    //for master node
    storage.isMaster = true
    storage.portnum = DEFAULT_MASTER_PORT
    storage.nodes = nodes
    storage.numnodes = numnodes

    //add masternode itself to nodes table
    hostport := fmt.Sprintf("localho:%d", DEFAULT_MASTER_PORT)
    self := storageproto.Node{hostport, nodeid}
    storage.nodes[self] = true
  } else {
    //for slave node
    storage.isMaster = false
    storage.portnum = portnum

    masterNode, err = rpc.DialHTTP("tcp", master)
    if lsplog.CheckReport(1, err) {
      return nil
    }

    for i := 0; (regReply.Ready == false) && (i < 5); i++ {
      time.Sleep(1000 * time.Millisecond)
      masterNode.Call("StorageRPC.Register", &regArgs, &regReply)
    }
  }

  storage.hash = make(map[string] []byte)

	return &storage
}

// Non-master servers to the master
func (ss *Storageserver) RegisterServer(args *storageproto.RegisterArgs,
                                    reply *storageproto.RegisterReply) error {

  if !ss.isMaster {
    lsplog.Vlogf(0, "WARNING:Calling a non-master node to register")
    return lsplog.MakeErr("Calling a non-master node to register")
  }

  _, present := ss.nodes[args.ServerInfo]
  if !present {
    //add to nodes
    ss.nodes[args.ServerInfo] = true
  }

  if len(ss.nodes) == ss.numnodes {
    reply.Ready = true
  } else {
    reply.Ready = false
  }
  reply.Servers = nil

	return nil
}

//dummy version
func (ss *Storageserver) GetServers(args *storageproto.GetServersArgs,
                                    reply *storageproto.RegisterReply) error {
  if !ss.isMaster {
    lsplog.Vlogf(0, "WARNING:Calling a non-master node for GetServers")
    return lsplog.MakeErr("Calling a non-master node to GetServers")
  }

  if len(ss.nodes) != ss.numnodes {
    reply.Ready = false
    reply.Servers = nil
    return nil
  }

  reply.Ready = true
  servers := make([]storageproto.Node, ss.numnodes)
  i := 0
  for node, _ := range ss.nodes {
    servers[i] = node
    i++
  }
	return nil
}

// RPC-able interfaces, bridged via StorageRPC.
// These should do something! :-)
func (ss *Storageserver) Get(args *storageproto.GetArgs,
                              reply *storageproto.GetReply) error {
  ss.rwlock.RLock()

  val, present := ss.hash[args.Key]
  if !present {
    reply.Status = storageproto.EKEYNOTFOUND
    ss.rwlock.RUnlock()
    return nil
  }

  err := json.Unmarshal([]byte(val), &(reply.Value))
  if err != nil {
    lsplog.Vlogf(0, "WARNING: unmarshal data generate an error")
  }

  lsplog.Vlogf(3, "Storage Get key %s, val %s", args.Key, reply.Value)
  reply.Status = storageproto.OK

  ss.rwlock.RUnlock()
	return nil
}

func (ss *Storageserver) GetList(args *storageproto.GetArgs,
                                    reply *storageproto.GetListReply) error {

  lsplog.Vlogf(3, "storage try to getlist with key %s", args.Key)

  ss.rwlock.RLock()

  val, present := ss.hash[args.Key]
  if !present {
      reply.Status = storageproto.EKEYNOTFOUND
      reply.Value = nil
      ss.rwlock.RUnlock()
      return nil
  }

  lsplog.Vlogf(3, "storage getlist key %s, val %s", args.Key, val)

  err := json.Unmarshal([]byte(val), &(reply.Value))
  if err != nil {
    lsplog.Vlogf(0, "WARNING: unmarshal data generate an error")
  }

  reply.Status = storageproto.OK

  ss.rwlock.RUnlock()
  return nil
}

func (ss *Storageserver) Put(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  var err error

  ss.rwlock.Lock()

  _, present := ss.hash[args.Key]
  if present {
    reply.Status = storageproto.EITEMEXISTS
    ss.rwlock.Unlock()
    return nil
  }

  if args.Value == "" {
    lsplog.Vlogf(3, "storage first put %s", args.Key)
    ss.hash[args.Key], err = json.Marshal([]string{})
  } else {
    lsplog.Vlogf(3, "storage put %s, val %s", args.Key, args.Value)
    ss.hash[args.Key], err = json.Marshal(args.Value)
  }

  if err != nil {
    lsplog.Vlogf(0, "WARNING: Marshal data generate an error")
  }

  reply.Status = storageproto.OK

  ss.rwlock.Unlock()

  return nil
}

func (ss *Storageserver) AppendToList(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  ss.rwlock.Lock()

  val, present := ss.hash[args.Key]
  if !present {
    lsplog.Vlogf(3, "try to append to %s list %s not exist", args.Key, args.Value)
    reply.Status = storageproto.EKEYNOTFOUND
    ss.rwlock.Unlock()
    return nil
  }

  lsplog.Vlogf(3, "storage append to %s list %s", args.Key, args.Value)

  var list []string;
  err := json.Unmarshal([]byte(val), &list)
  if err != nil {
    lsplog.Vlogf(0, "WARNING: unmarshal data generate an error")
  }

  //need check duplicate before insertion
  for _, v := range list {
    if v == args.Value {
      reply.Status = storageproto.EITEMEXISTS
      ss.rwlock.Unlock()
      return nil
    }
  }

  list = append([]string{args.Value}, list...)

  ss.hash[args.Key], err = json.Marshal(list)
  if err != nil {
    lsplog.Vlogf(0, "WARNING: Marshal data generate an error")
  }

  reply.Status = storageproto.OK

  ss.rwlock.Unlock()

	return nil
}

func (ss *Storageserver) RemoveFromList(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  lsplog.Vlogf(0, "removeFromList key %s", args.Key)

  ss.rwlock.Lock()

  val, present := ss.hash[args.Key]
  if !present {
      lsplog.Vlogf(3, "try to remove, key %s does not exist", args.Key)
      reply.Status = storageproto.EKEYNOTFOUND
      ss.rwlock.Unlock()
      return nil
  }

  var list []string;
  err := json.Unmarshal([]byte(val), &list)
  if err != nil {
    lsplog.Vlogf(0, "WARNING: unmarshal data generate an error")
  }

  for i, v := range list {
    if v == args.Value {
      list = append(list[:i], list[i+1:]...)

      ss.hash[args.Key], err = json.Marshal(list)
      if err != nil {
        lsplog.Vlogf(0, "WARNING: Marshal data generate an error")
      }

      reply.Status = storageproto.OK
      ss.rwlock.Unlock()
      return nil
    }
  }

  reply.Status = storageproto.EITEMNOTFOUND

  ss.rwlock.Unlock()
	return nil
}

func (ss *Storageserver) RevokeLease(*storageproto.RevokeLeaseArgs,
                                      *storageproto.RevokeLeaseReply) error {
  return nil
}
