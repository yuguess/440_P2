package tribimpl

import (
  "encoding/json"
  "fmt"
  "time"
	"P2-f12/official/tribproto"
  "P2-f12/contrib/libstore"
  "P2-f12/official/lsplog"
)

type Tribserver struct {
  Store *libstore.Libstore
  Id uint32
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

  svr.Id = 0

	return svr
}

func (ts *Tribserver) CreateUser(
    args *tribproto.CreateUserArgs, reply *tribproto.CreateUserReply) error {
  var trib_key, fllw_key string
  var err error

  trib_key = fmt.Sprintf("%s:T", args.Userid)
  fllw_key = fmt.Sprintf("%s:F", args.Userid)

  _, err = ts.Store.GetList(trib_key)
  if !lsplog.CheckReport(1, err) {
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
  var fllw_key string
  var err error

  fllw_key = fmt.Sprintf("%s:F", args.Userid)
  err = ts.Store.AppendToList(fllw_key, args.Targetuser)

  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.ENOSUCHTARGETUSER
  } else {
    reply.Status = tribproto.OK
  }

	return nil
}

func (ts *Tribserver) RemoveSubscription(
    args *tribproto.SubscriptionArgs,
    reply *tribproto.SubscriptionReply) error {
  var fllw_key string
  var err error

  fllw_key = fmt.Sprintf("%s:F", args.Userid)
  err = ts.Store.RemoveFromList(fllw_key, args.Targetuser)

  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.ENOSUCHTARGETUSER
  } else {
    reply.Status = tribproto.OK
  }

	return nil
}

func (ts *Tribserver) GetSubscriptions(
    args *tribproto.GetSubscriptionsArgs,
    reply *tribproto.GetSubscriptionsReply) error {
  var fllw_key string
  var fllw_ids []string
  var err error

  fllw_key = fmt.Sprintf("%s:F", args.Userid)

  fllw_ids, err = ts.Store.GetList(fllw_key)
  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.ENOSUCHUSER
    reply.Userids = nil
	  return err
  }

  reply.Status = tribproto.OK
  reply.Userids = fllw_ids

	return nil
}

func (ts *Tribserver) PostTribble(
    args *tribproto.PostTribbleArgs, reply *tribproto.PostTribbleReply) error {
  var trib_key string
  var trib tribproto.Tribble
  var enc []byte
  var err error

  trib_key = fmt.Sprintf("%s:T", args.Userid)

  err = ts.Store.AppendToList(trib_key, string(ts.Id))
  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.ENOSUCHUSER
    return err
  }

  trib.Userid = args.Userid
  trib.Posted = time.Now()
  trib.Contents = args.Contents

  enc, err = json.Marshal(trib)
  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.OK
    return err
  }

  err = ts.Store.Put(string(ts.Id), string(enc))
  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.OK
    return err
  }

  reply.Status = tribproto.OK
  ts.Id++

	return nil
}

func (ts *Tribserver) GetTribbles(
    args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) error {
  var trib_key string
  var trib_ids []string
  var trib_enc string
  var err error

  trib_key = fmt.Sprintf("%s:T", args.Userid)

  trib_ids, err = ts.Store.GetList(trib_key)
  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.ENOSUCHUSER
    reply.Tribbles = nil
    return nil
  }

  reply.Status = tribproto.OK
  reply.Tribbles = make([]tribproto.Tribble, len(trib_ids))

  for i := 0; i < len(trib_ids); i++ {
    trib_enc, err = ts.Store.Get(trib_ids[i])
    _ = json.Unmarshal([]byte(trib_enc), reply.Tribbles[i])
  }

	return nil
}

// collect all tribbles from all users followed
// TODO: trim the list by sorting and taking only the most recent 100 tribbles
func (ts *Tribserver) GetTribblesBySubscription(
    args *tribproto.GetTribblesArgs, reply *tribproto.GetTribblesReply) error {
  var fllw_key string
  var fllw_ids []string
  var err error
  var getArgs tribproto.GetTribblesArgs
  var getReply tribproto.GetTribblesReply

  fllw_key = fmt.Sprintf("%s:F", args.Userid)

  fllw_ids, err = ts.Store.GetList(fllw_key)
  if lsplog.CheckReport(1, err) {
    reply.Status = tribproto.ENOSUCHUSER
    reply.Tribbles = nil
    return nil
  }

  reply.Status = tribproto.OK

  for i := 0; i < len(fllw_ids); i++ {
    err = ts.GetTribbles(&getArgs, &getReply)
    if lsplog.CheckReport(1, err) {
      reply.Status = tribproto.ENOSUCHTARGETUSER
      reply.Tribbles = nil
      return nil
    }

    reply.Tribbles = append(reply.Tribbles, getReply.Tribbles...)
  }

	return nil
}
