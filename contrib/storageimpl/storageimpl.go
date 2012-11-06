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

type leaseHolder struct {
  holderAddr string
  issueTime time.Time
}

type leaseEntry struct {
  holders []leaseHolder
  pending *bool
  mtx *sync.Mutex
}

type Storageserver struct {
  hash map[string] []byte
  portnum int
  nodeid uint32
  isMaster bool //identify whether this node is master node
  nodes map[storageproto.Node] bool //master node store all other servers info
  numnodes int
  //rwlock sync.RWMutex //reader writer lock 

  leasePool map[string] leaseEntry
}

func reallySeedTheDamnRNG() {
	//randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	//rand.Seed( randint.Int64())
}

func NewStorageserver(master string, numnodes int, portnum int,
                                        nodeid uint32) *Storageserver {

  lsplog.SetVerbose(3)
  fmt.Println("Create New Storage Server")
  fmt.Printf("master:%s, numnodes:%d, portnum:%d, nodeid:%d\n",
                                      master, numnodes, portnum, nodeid)
  var masterNode *rpc.Client
  var regArgs storageproto.RegisterArgs
  var regReply storageproto.RegisterReply
  var storage Storageserver
  var err error
  var nodes = make(map[storageproto.Node] bool)

  storage.nodeid = nodeid
  storage.leasePool = make(map[string] leaseEntry)

  selfAddr := fmt.Sprintf("localhost:%d",portnum)

  if master == selfAddr {
    fmt.Printf("for master node\n")

    //for master node
    storage.isMaster = true
    storage.portnum = portnum
    //storage.portnum = DEFAULT_MASTER_PORT
    storage.nodes = nodes
    storage.numnodes = numnodes

    //add masternode itself to nodes table
    //hostport := fmt.Sprintf("localhost:%d", DEFAULT_MASTER_PORT)
    self := storageproto.Node{master, nodeid}
    storage.nodes[self] = true
  } else {

    masterNode, err = rpc.DialHTTP("tcp", master)
    if lsplog.CheckReport(1, err) {
      return nil
    }

    regArgs.ServerInfo.HostPort = fmt.Sprintf("localhost:%d", portnum)
    regArgs.ServerInfo.NodeID = nodeid

    //for slave node
    storage.isMaster = false
    storage.portnum = portnum

    fmt.Printf("for slave node\n")
    fmt.Printf("begin try $$$\n")

    for i := 0; (regReply.Ready == false) && (i < 10); i++ {
      fmt.Printf("try %d times\n", i)
      masterNode.Call("StorageRPC.Register", &regArgs, &regReply)
      /*
      if lsplog.CheckReport(1, err) {
        lsplog.Vlogf(3, "slave %d call RegisterServer %d time failed",
                                                    i + 1, portnum)
      }*/
      time.Sleep(1000 * time.Millisecond)
    }
  }

  storage.hash = make(map[string] []byte)

	return &storage
}

// Non-master servers to the master
func (ss *Storageserver) RegisterServer(args *storageproto.RegisterArgs,
                                    reply *storageproto.RegisterReply) error {
  fmt.Printf("st registerServer invoked\n")

  if !ss.isMaster {
    lsplog.Vlogf(0, "WARNING:Calling a non-master node to register")
    return lsplog.MakeErr("Calling a non-master node to register")
  }

  _, present := ss.nodes[args.ServerInfo]
  if !present {
    //add to nodes
    ss.nodes[args.ServerInfo] = true
    fmt.Println("add nodes %v", args.ServerInfo)
  }

  fmt.Printf("master collect slave info %d/%d\n", len(ss.nodes), ss.numnodes)

  reply.Servers = nil

  if len(ss.nodes) == ss.numnodes {
    reply.Ready = true
    //ss.GetServers(nil, reply)
    servers := make([]storageproto.Node, ss.numnodes)
    i := 0
    for node, _ := range ss.nodes {
      //fmt.Printf("i: %d, info: %v\n", i, node)
      servers[i] = node
      i++
    }
  reply.Servers = servers
  } else {
    reply.Ready = false
  }

	return nil
}

func (ss *Storageserver) GetServers(args *storageproto.GetServersArgs,
                                    reply *storageproto.RegisterReply) error {

  fmt.Println("Storage GetServers invoked")

  if !ss.isMaster {
    fmt.Println("WARNING:Calling a non-master node for GetServers")
    return lsplog.MakeErr("Calling a non-master node to GetServers")
  }

  if len(ss.nodes) != ss.numnodes {
    fmt.Println("GetServer not ready")

    //what a hack here, need change if time possible
    time.Sleep(time.Duration(2 * 1000) * time.Millisecond)

    reply.Ready = false
    return nil
  }

  servers := make([]storageproto.Node, ss.numnodes)
  i := 0
  for node, _ := range ss.nodes {
    //fmt.Printf("i: %d, info: %v\n", i, node)
    servers[i] = node
    i++
  }
  reply.Servers = servers
  reply.Ready = true

  return nil
}

func isTimeout(holder leaseHolder) bool {
  dur := time.Since(holder.issueTime).Seconds()
  if dur > (storageproto.LEASE_SECONDS + storageproto.LEASE_GUARD_SECONDS) {
    return true
  }

  return false
}

func search(list []leaseHolder, addr string) *leaseHolder {
  for _, v := range list {
    if v.holderAddr == addr {
      return &v
    }
  }
  return nil
}

func (ss *Storageserver) addLeasePool(args *storageproto.GetArgs,
                                  lease *storageproto.LeaseStruct) error {

  fmt.Printf("add key %s to lease pool\n", args.Key)

  var tmp = false
  var mtx sync.Mutex

  entry, present := ss.leasePool[args.Key]

  lease.Granted = true
  lease.ValidSeconds = storageproto.LEASE_SECONDS

  if present {
    //make sure do not grant duplicate lease 
    holder := search(entry.holders, args.LeaseClient)
    if holder != nil {

      fmt.Printf("holder isTimeout %t, pending %t\n", isTimeout(*holder) ,
                                                  *(entry.pending))

      if isTimeout(*holder) || !(*(entry.pending))  {
        holder.issueTime = time.Now()
      } else {
        lease.Granted = false
        lease.ValidSeconds = 0
        return lsplog.MakeErr("trying to issue duplicate lease")
      }

      return nil
    }
  } else {
    ss.leasePool[args.Key] = leaseEntry{nil, nil, &mtx}
    entry = ss.leasePool[args.Key]
    entry.pending = &tmp
  }

  holder := make([]leaseHolder, 1)
  holder[0] = leaseHolder{args.LeaseClient, time.Now()}

  entry.holders = append(holder, (entry.holders)...)
  (ss.leasePool[args.Key]) = entry
  return nil
}

// RPC-able interfaces, bridged via StorageRPC.
// These should do something! :-)
func (ss *Storageserver) Get(args *storageproto.GetArgs,
                              reply *storageproto.GetReply) error {
  //ss.rwlock.RLock()
  fmt.Printf("try to GET key %s\n", args.Key)

  val, present := ss.hash[args.Key]
  if !present {
    //if the whole system only have one storage node
    if ss.numnodes == 1 {
      reply.Status = storageproto.EKEYNOTFOUND
    } else {
      //reply.Status = storageproto.EKEYNOTFOUND
      reply.Status = storageproto.EWRONGSERVER
    }

    fmt.Printf("storage GET key %s failed, nonexist\n", args.Key)
    //ss.rwlock.RUnlock()
    return nil
  }

  err := json.Unmarshal([]byte(val), &(reply.Value))
  if err != nil {
    lsplog.Vlogf(0, "WARNING: unmarshal data generate an error")
  }

  if args.WantLease {
    ss.addLeasePool(args, &(reply.Lease))
  }

  fmt.Printf("Storage Get key %s, val %s, lease %t\n",
                                args.Key, reply.Value, reply.Lease.Granted)
  reply.Status = storageproto.OK
  //ss.rwlock.RUnlock()
	return nil
}

func (ss *Storageserver) GetList(args *storageproto.GetArgs,
                                    reply *storageproto.GetListReply) error {

  lsplog.Vlogf(3, "storage try to getlist with key %s", args.Key)

  //ss.rwlock.RLock()

  val, present := ss.hash[args.Key]
  if !present {
    if ss.numnodes == 1 {
      reply.Status = storageproto.EKEYNOTFOUND
    } else {
      reply.Status = storageproto.EWRONGSERVER
    }
    reply.Value = nil
    //ss.rwlock.RUnlock()
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

  //ss.rwlock.RUnlock()
  return nil
}

func (ss *Storageserver) Put(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  var err error

  fmt.Printf("st svr put invoked key %s, val %s !!!\n", args.Key, args.Value)

  //ss.rwlock.Lock()
  if entry, present := ss.leasePool[args.Key]; present {
    fmt.Printf("try to put to %s still lease pool, call revoke!!!\n", args.Key)
    entry.mtx.Lock()
    ss.revokeLeaseHolders(args.Key)
    entry.mtx.Unlock()
  }

  _, present := ss.hash[args.Key]
  if present {
    ss.hash[args.Key], _ = json.Marshal(args.Value)
    reply.Status = storageproto.OK
    //ss.rwlock.Unlock()
    return nil
  }

  if args.Value == "" {
    lsplog.Vlogf(3, "storage first put %s", args.Key)
    ss.hash[args.Key], err = json.Marshal([]string{})
  } else {
    //fmt.Printf("storage put %s, val %s", args.Key, args.Value)
    ss.hash[args.Key], err = json.Marshal(args.Value)
  }

  if err != nil {
    lsplog.Vlogf(0, "WARNING: Marshal data generate an error")
  }

  reply.Status = storageproto.OK

  //ss.rwlock.Unlock()
  //fmt.Println("storage put complete!")
  return nil
}

func boundedWaitCall(args *storageproto.RevokeLeaseArgs,
  reply *storageproto.RevokeLeaseReply, con *rpc.Client, doneChan chan int) {

  err := con.Call("CacheRPC.RevokeLease", &args, &reply)
  if lsplog.CheckReport(1, err) {
      fmt.Printf("Try revoke lease holder failed\n!")
  }

  doneChan <- 1
  return
}

func (ss *Storageserver) revokeLeaseHolders(key string) error {
  var args storageproto.RevokeLeaseArgs
  var reply storageproto.RevokeLeaseReply
  var doneChan chan int

  entry := (ss.leasePool[key])

  (*(entry.pending)) = true
  fmt.Printf("set key %s pending\n", key)

  for _, holder := range entry.holders {

    if isTimeout(holder) {
      fmt.Printf("revoke holder %s expire\n", key)
      continue;
    }

    svr, err := rpc.DialHTTP("tcp", holder.holderAddr)
    if lsplog.CheckReport(1, err) {
      fmt.Printf("revoke dial %s failed", holder.holderAddr)
    }

    args.Key = key

    //ensure rpc is bounded waiting
    go boundedWaitCall(&args, &reply, svr, doneChan)

    select {
    case <- doneChan:
          break
    case <- time.After((storageproto.LEASE_SECONDS +
                        storageproto.LEASE_GUARD_SECONDS) * time.Second):
		      break
    }

    fmt.Printf("revoke complete rpc to holder %s\n", holder.holderAddr)
  }
  (*(entry.pending)) = false
  fmt.Printf("cannel key %s pending !\n", key)
  return nil
}

func (ss *Storageserver) AppendToList(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {

  fmt.Printf("try append %s to list %s\n", args.Value, args.Key)

  //ss.rwlock.Lock()

  _, present := ss.hash[args.Key]
  if !present {
    ss.hash[args.Key] = nil
    /*
    fmt.Printf("try append %s list with %s, list not exist\n",args.Key, args.Value)
    reply.Status = storageproto.EKEYNOTFOUND
    ss.rwlock.Unlock()
    return nil
    */
  }

  entry, present := ss.leasePool[args.Key]

  if present {
    //this mutex will ''queue'' later put request while revoking
    entry.mtx.Lock()
    ss.revokeLeaseHolders(args.Key)
  }

  fmt.Printf("storage append to %s list %s\n", args.Key, args.Value)

  var list []string;
  err := json.Unmarshal([]byte(ss.hash[args.Key]), &list)
  if err != nil {
    lsplog.Vlogf(0, "WARNING: unmarshal data generate an error")
  }

  //need check duplicate before insertion
  for _, v := range list {
    if v == args.Value {
      reply.Status = storageproto.EITEMEXISTS
      //ss.rwlock.Unlock()
      return nil
    }
  }

  list = append(list, ([]string{args.Value})...)

  ss.hash[args.Key], err = json.Marshal(list)
  if err != nil {
    lsplog.Vlogf(0, "WARNING: Marshal data generate an error")
  }

  reply.Status = storageproto.OK

  fmt.Printf("comp apd %s to %s,val %s\n", args.Value, args.Key, ss.hash[args.Key])

  //ss.rwlock.Unlock()
  if present {
    entry.mtx.Unlock()
  }

	return nil
}

func (ss *Storageserver) RemoveFromList(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  lsplog.Vlogf(0, "removeFromList key %s", args.Key)

  //ss.rwlock.Lock()

  val, present := ss.hash[args.Key]
  if !present {
      lsplog.Vlogf(3, "try to remove, key %s does not exist", args.Key)
      reply.Status = storageproto.EKEYNOTFOUND
      //ss.rwlock.Unlock()
      return nil
  }

  entry, present := ss.leasePool[args.Key]

  if present {
    entry.mtx.Lock()
    ss.revokeLeaseHolders(args.Key)
    
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
      //ss.rwlock.Unlock()
      return nil
    }
  }

  reply.Status = storageproto.EITEMNOTFOUND
  if present {
    entry.mtx.Unlock()
  }
  //ss.rwlock.Unlock()
	return nil
}

func (ss *Storageserver) RevokeLease(*storageproto.RevokeLeaseArgs,
                                      *storageproto.RevokeLeaseReply) error {
  return nil
}
