package tribimpl

import (
	"P2-f12/official/tribproto"
    "P2-f12/contrib/libstore"
    "P2-f12/official/lsplog"
)

type Tribserver struct {
    store  *libstore.Libstore
}

func NewTribserver(storagemaster string, myhostport string) *Tribserver {
    st, _ := libstore.NewLibstore("test", "test", 0)
	return &Tribserver{st}
}

func (ts *Tribserver) CreateUser(args *tribproto.CreateUserArgs,
                                reply *tribproto.CreateUserReply) error {
    ts.store.Get("test")
    lsplog.Vlogf(0, "server createUser method was invoked !")
	// Set responses by modifying the reply structure, like:
	// reply.Status = tribproto.EEXISTS
	return nil
}

func (ts *Tribserver) AddSubscription(args *tribproto.SubscriptionArgs,
                                    reply *tribproto.SubscriptionReply) error {
	return nil
}

func (ts *Tribserver) RemoveSubscription(args *tribproto.SubscriptionArgs,
                                    reply *tribproto.SubscriptionReply) error {
	return nil
}

func (ts *Tribserver) GetSubscriptions(args *tribproto.GetSubscriptionsArgs,
                                reply *tribproto.GetSubscriptionsReply) error {
	return nil
}

func (ts *Tribserver) PostTribble(args *tribproto.PostTribbleArgs,
                                    reply *tribproto.PostTribbleReply) error {
	return nil
}

func (ts *Tribserver) GetTribbles(args *tribproto.GetTribblesArgs,
                                    reply *tribproto.GetTribblesReply) error {
	return nil
}

func (ts *Tribserver) GetTribblesBySubscription(args *tribproto.GetTribblesArgs,
                                    reply *tribproto.GetTribblesReply) error {
	return nil
}
