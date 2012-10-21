// Your implementation of the libstore should go here.
package libstore

import (
//  "fmt"
  "net"
  "net/rpc"
	"P2-f12/official/lsplog"
  "P2-f12/official/storageproto"
)

type Libstore struct {
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
}

func (ls *Libstore) iGet(key string) (string, error) {
  args := &storageproto.GetArgs{key, false, ""}
  reply := &storageproto.GetReply{}

  ls.store_clnt.Call("StorageRPC.Get", args, reply)

  if reply.Status == storageproto.EKEYNOTFOUND {
    return  "", lsplog.MakeErr("Get key not found")
  }

  return reply.Value, nil
}

func (ls *Libstore) iPut(key, value string) error {
  args := &storageproto.PutArgs{key, value}
  reply := &storageproto.PutReply{}

  ls.store_clnt.Call("StorageRPC.Put", args, reply)

  if reply.Status == storageproto.EITEMEXISTS {
    return  lsplog.MakeErr("Duplicate Put !")
  }

	return nil

	//return lsplog.NotImplemented("iPut")
}

func (ls *Libstore) iGetList(key string) ([]string, error) {
  args := &storageproto.GetArgs{key, false, ""}
  reply := &storageproto.GetListReply{}

  ls.store_clnt.Call("StorageRPC.GetList", args, reply)

  if reply.Status == storageproto.EKEYNOTFOUND {
    return  nil, lsplog.MakeErr("GetList key not found")
  }

	return reply.Value, nil
}

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
}

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
}
