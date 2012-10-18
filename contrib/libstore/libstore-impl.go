// Your implementation of the libstore should go here.
package libstore

import (
	"P2-f12/official/lsplog"
)

type Libstore struct {
}

func iNewLibstore(server, myhostport string, flags int) (*Libstore, error) {
	return nil, lsplog.NotImplemented("iNewLibstore")
}

func (ls *Libstore) iGet(key string) (string, error) {
	return "", lsplog.NotImplemented("iGet")
}

func (ls *Libstore) iPut(key, value string) error {
	return lsplog.NotImplemented("iPut")
}

func (ls *Libstore) iGetList(key string) ([]string, error) {
	return nil, lsplog.NotImplemented("iGetList")
}

func (ls *Libstore) iRemoveFromList(key, removeitem string) error {
	return lsplog.NotImplemented("iRemoveFromList")
}

func (ls *Libstore) iAppendToList(key, newitem string) error {
	return lsplog.NotImplemented("iAppendToList")
}
