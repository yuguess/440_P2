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

type leaseEntry struct {
  holderAddr string
  issueTime time.Time
}

type Storageserver struct {
  hash map[string] []byte
  portnum int
  nodeid uint32
  isMaster bool //identify whether this node is master node
  nodes map[storageproto.Node] bool //master node store all other servers info
  numnodes int
  rwlock sync.RWMutex //reader writer lock 

  leasePool map[string] []leaseEntry
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
  storage.leasePool = make(map[string] []leaseEntry)

  lsplog.Vlogf(3, "master %s", master)

  if master == "" || numnodes == 1{
    //for master node
    storage.isMaster = true
    storage.portnum = DEFAULT_MASTER_PORT
    storage.nodes = nodes
    storage.numnodes = numnodes

    //add masternode itself to nodes table
    hostport := fmt.Sprintf("localhost:%d", DEFAULT_MASTER_PORT)
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

func (ss *Storageserver) GetServers(args *storageproto.GetServersArgs,
                                    reply *storageproto.RegisterReply) error {

  lsplog.Vlogf(0, "Storage GetServer invoked")

  if !ss.isMaster {
    lsplog.Vlogf(0, "WARNING:Calling a non-master node for GetServers")
    return lsplog.MakeErr("Calling a non-master node to GetServers")
  }

  if len(ss.nodes) != ss.numnodes {
    lsplog.Vlogf(3, "GetServer not ready")
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
  reply.Servers = servers
	return nil
}

func isTimeout(entry leaseEntry) bool {
  dur := time.Since(entry.issueTime).Seconds()
  if dur > (storageproto.LEASE_SECONDS + storageproto.LEASE_GUARD_SECONDS) {
    return true
  }

  return false
}

func search(list []leaseEntry, addr string) *leaseEntry{
  for _, v := range list {
    if v.holderAddr == addr {
      return &v
    }
  }
  return nil
}

func (ss *Storageserver) addLeasePool(args *storageproto.GetArgs,
                                  lease *storageproto.LeaseStruct) error {

  list, present := ss.leasePool[args.Key]

  lease.Granted = true
  lease.ValidSeconds = storageproto.LEASE_SECONDS

  if present {
    //make sure do not grant duplicate lease 
    holder := search(list, args.LeaseClient)
    if holder != nil {
      if isTimeout(*holder) {
        holder.issueTime = time.Now()
      } else {
        lsplog.Vlogf(0, "WARNING: issue duplicate lease")
        lease.Granted = false
        lease.ValidSeconds = 0
        return lsplog.MakeErr("trying to issue duplicate lease")
      }
      return nil
    }
  }

  entry := make([]leaseEntry, 1)
  entry[0] = leaseEntry{args.LeaseClient, time.Now()}
  ss.leasePool[args.Key] = append(entry, list...)

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

  if args.WantLease {
    ss.addLeasePool(args, &(reply.Lease))
  }

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

  if args.WantLease {
    ss.addLeasePool(args, &(reply.Lease))
  }

  ss.rwlock.RUnlock()
  return nil
}

func (ss *Storageserver) Put(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  var err error

  lsplog.Vlogf(0, "storage put invoked!")

  ss.rwlock.Lock()

  _, present := ss.hash[args.Key]
  if present {
    reply.Status = storageproto.EITEMEXISTS
    ss.rwlock.Unlock()
    return nil
  }

  if holders, present := ss.leasePool[args.Key]; present {
    revokeLeaseHolders(args.Key, holders)
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
  lsplog.Vlogf(0, "storage put complete!")
  return nil
}

func revokeLeaseHolders(key string, holders []leaseEntry) error {
  var args *storageproto.RevokeLeaseArgs
  var reply *storageproto.RevokeLeaseReply

  for _, entry := range holders {
    if isTimeout(entry) {
      continue;
    }

    svr, err := rpc.DialHTTP("tcp", entry.holderAddr)
    if lsplog.CheckReport(1, err) {
      return nil
    }

    err = svr.Call("Libstore.RevokeLease", &args, &reply)
    if lsplog.CheckReport(1, err) {
      lsplog.Vlogf(0,
              "WARNING: try revoke lease holder %d failed !", entry.holderAddr)
      return err
    }
  }
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

  if holders, present := ss.leasePool[args.Key]; present {
    revokeLeaseHolders(args.Key, holders)
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

  if holders, present := ss.leasePool[args.Key]; present {
    revokeLeaseHolders(args.Key, holders)
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
