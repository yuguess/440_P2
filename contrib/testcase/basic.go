package main

import (
	//"log"
	"fmt"
	//"flag"
	"P2-f12/official/tribproto"
  "P2-f12/official/lsplog"
	//"strings"
	//"time"
)

func PrintTribbles(tribbles []tribproto.Tribble) {
	for _, t := range tribbles {
		PrintTribble(t)
	}
}

func PrintTribble(t tribproto.Tribble) {
	fmt.Printf("%16.16s - %s - %s\n",
		t.Userid, t.Posted.String(), t.Contents)
}

func main() {

  serverAddress := "127.0.0.1"
	serverPort := fmt.Sprintf("%d", 9010)
  client, _ := NewTribbleclient(serverAddress, serverPort)

  //create user
  status, err := client.CreateUser("1")
  fmt.Printf("status:%d\n", status)
  if lsplog.CheckReport(0, err) {
    return
  }

//  //test duplicate create user !
//  status, err = client.CreateUser("1")
//  fmt.Printf("status:%d\n", status)
//
//  //test nonexist user post message
//  status, err = client.PostTribble("2", "test1")
//  fmt.Printf("status:%d\n", status)
//  if lsplog.CheckReport(0, err) {
//    return
//  }

  //test post msg
  status, err = client.PostTribble("1", "test1")
  fmt.Printf("status:%d\n", status)

  //test get msg
  tribbles, status, err := client.GetTribbles("1")
  fmt.Printf("status:%d\n", status)
  PrintTribbles(tribbles)

  //create user 2
  status, err = client.CreateUser("2")
  fmt.Printf("status:%d\n", status)

  //user2 post message
  status, err = client.PostTribble("2", "test2")
  fmt.Printf("status:%d\n", status)

  //user2 get message
  tribbles, status, err = client.GetTribbles("2")
  fmt.Printf("status:%d\n", status)
  PrintTribbles(tribbles)

  //test user1 addsubscription user2
  status, err = client.AddSubscription("1", "2")
  fmt.Printf("status:%d\n", status)

//  //test user1 getsubscription 
//  users, status, err := client.GetSubscriptions("1")
//  fmt.Printf("status:%d\n", status)
//  fmt.Println(strings.Join(users, ","))


  //test GetTribblesBySubscription
  tribbles, status, err = client.GetTribblesBySubscription("1")
  fmt.Printf("status:%d\n", status)
  PrintTribbles(tribbles)

  //test removesubscription
  status, err = client.RemoveSubscription("1", "2")
  fmt.Printf("status:%d\n", status)

  //test GetTribblesBySubscription
  tribbles, status, err = client.GetTribblesBySubscription("1")
  fmt.Printf("status:%d\n", status)
  PrintTribbles(tribbles)

}
