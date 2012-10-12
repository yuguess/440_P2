/** @file libstore-impl.go
 *  @brief implementation of libstore 
 *  @author Adrian() Dalong CHENG (dalongc)
 *  @date 2012-10-11
 */
package libstore

import (
	"P2-f12/official/lsplog"
)

type Libstore struct {
}

func iNewLibstore(server, myhostport string, flags int) (*Libstore, error) {
    return &Libstore{}, nil
	//return nil, lsplog.NotImplemented("iNewLibstore")
}

func (ls *Libstore) iGet(key string) (string, error) {
    lsplog.Vlogf(0, "libstore iGet method was invoked ! key %s", key)
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
