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
  //"fmt"
  "P2-f12/official/storageproto"
  "P2-f12/official/lsplog"
  "encoding/json"
  //"P2-f12/official/tribproto"
  //"math"
  //"math/big"
  //"math/rand"
)

type Storageserver struct {
  hash map[string] []byte
}

func reallySeedTheDamnRNG() {
	//randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	//rand.Seed( randint.Int64())
}

func NewStorageserver(master string, numnodes int, portnum int,
                                        nodeid uint32) *Storageserver {
  lsplog.SetVerbose(3)
  lsplog.Vlogf(3, "Create New Storage Server")

  hash := make(map[string] []byte)
	return &Storageserver{hash}
}

// Non-master servers to the master
func (ss *Storageserver) RegisterServer(args *storageproto.RegisterArgs,
                                    reply *storageproto.RegisterReply) error {
	return nil
}

func (ss *Storageserver) GetServers(args *storageproto.GetServersArgs,
                                    reply *storageproto.RegisterReply) error {
	return nil
}

// RPC-able interfaces, bridged via StorageRPC.
// These should do something! :-)

func (ss *Storageserver) Get(args *storageproto.GetArgs,
                              reply *storageproto.GetReply) error {

  val, present := ss.hash[args.Key]
  if !present {
    reply.Status = storageproto.EKEYNOTFOUND
    return nil
  }

  err := json.Unmarshal([]byte(val), &(reply.Value))
  if err != nil {
    lsplog.Vlogf(0, "WARNING: unmarshal data generate an error")
  }

  lsplog.Vlogf(3, "Storage Get key %s, val %s", args.Key, reply.Value)
  reply.Status = storageproto.OK

	return nil
}

func (ss *Storageserver) GetList(args *storageproto.GetArgs,
                                    reply *storageproto.GetListReply) error {

  lsplog.Vlogf(3, "storage try to getlist with key %s", args.Key)

  val, present := ss.hash[args.Key]
  if !present {
      reply.Status = storageproto.EKEYNOTFOUND
      reply.Value = nil
      return nil
  }

  lsplog.Vlogf(3, "storage getlist key %s, val %s", args.Key, val)

  err := json.Unmarshal([]byte(val), &(reply.Value))
  if err != nil {
    lsplog.Vlogf(0, "WARNING: unmarshal data generate an error")
  }

  reply.Status = storageproto.OK
  return nil
}

func (ss *Storageserver) Put(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  var err error
  _, present := ss.hash[args.Key]
  if present {
    reply.Status = storageproto.EITEMEXISTS
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
  return nil
}

func (ss *Storageserver) AppendToList(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  val, present := ss.hash[args.Key]
  if !present {
    lsplog.Vlogf(3, "try to append to %s list %s not exist", args.Key, args.Value)
      reply.Status = storageproto.EKEYNOTFOUND
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
      return nil
    }
  }

  list = append(list, []string{args.Value}...)

  ss.hash[args.Key], err = json.Marshal(list)
  if err != nil {
    lsplog.Vlogf(0, "WARNING: Marshal data generate an error")
  }

  reply.Status = storageproto.OK

	return nil
}

func (ss *Storageserver) RemoveFromList(args *storageproto.PutArgs,
                                        reply *storageproto.PutReply) error {
  lsplog.Vlogf(0, "removeFromList key %s", args.Key)

  val, present := ss.hash[args.Key]
  if !present {
      lsplog.Vlogf(3, "try to remove, key %s does not exist", args.Key)
      reply.Status = storageproto.EKEYNOTFOUND
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
      return nil
    }
  }

  reply.Status = storageproto.EITEMNOTFOUND
	return nil
}

func (ss *Storageserver) RevokeLease(*storageproto.RevokeLeaseArgs,
                                      *storageproto.RevokeLeaseReply) error {
  return nil
}
