package tribimpl

import (
  "fmt"
	"P2-f12/official/tribproto"
  "P2-f12/official/libstore"
  "P2-f12/official/lsplog"
)

type Tribserver struct {
  Store *libstore.Libstore
}

func NewTribserver(storagemaster, myhostport string) *Tribserver {
  var svr *Tribserver = new(Tribserver)
  var err error

  // libstore.NONE forces no leases on Get and GetList requests
  svr.Store, err =
      libstore.NewLibstore(storagemaster, myhostport, libstore.NONE)

  if lsplog.CheckReport(1, err) {
    return nil
  }

	return svr
}

func (ts *Tribserver) CreateUser(
    args *tribproto.CreateUserArgs, reply *tribproto.CreateUserReply) error {
  var trib_key, fllw_key string
  var err error

  trib_key = fmt.Sprintf("%s_T", args.Userid)
  fllw_key = fmt.Sprintf("%s_F", args.Userid)

  _, err = ts.Store.GetList(trib_key)
  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.EEXISTS
	  return nil
  }

  err = ts.Store.Put(trib_key, "")
  if lsplog.CheckReport(2, err) {
    reply.Status = tribproto.EEXISTS
    return err
  }

  err = ts.Store.Put(fllw_key, "")
  if lsplog.CheckReport(2, err) {
    reply.Status = tribproto.EEXISTS
    return err
  }

  reply.Status = tribproto.OK

  return nil
}

func (ts *Tribserver) AddSubscription(
    args *tribproto.SubscriptionArgs,
    reply *tribproto.SubscriptionReply) error {
	return nil
}

func (ts *Tribserver) RemoveSubscription(
    args *tribproto.SubscriptionArgs,
    reply *tribproto.SubscriptionReply) error {
	return nil
}

func (ts *Tribserver) GetSubscriptions(
    args *tribproto.GetSubscriptionsArgs,
    reply *tribproto.GetSubscriptionsReply) error {
	return nil
}

func (ts *Tribserver) PostTribble(
    args *tribproto.PostTribbleArgs, reply *tribproto.PostTribbleReply) error {
	return nil
}

func (ts *Tribserver) GetTribbles(
    args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) error {
	return nil
}

func (ts *Tribserver) GetTribblesBySubscription(
    args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) error {
	return nil
}
