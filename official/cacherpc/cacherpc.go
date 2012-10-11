// Do not modify this file.
//
// Calls into your own implementation's functions.

import (
	"P2-f12/official/storageproto"
)

type CacherInterface {
	RevokeLease(*storageproto.RevokeLeaseArgs, *storageproto.RevokeLeaseReply) error
}

type CacheRPC struct {
	c CacherInterface
}

func (crpc *StorageRPC) RevokeLease(args *storageproto.RevokeLeaseArgs, reply *storageproto.RevokeLeaseReply) error {
        return crpc.c.RevokeLease(args, reply)
}
