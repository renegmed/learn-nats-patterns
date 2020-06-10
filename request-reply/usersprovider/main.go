package main

import (
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/mux"
	nats "github.com/nats-io/nats.go"
	"github.com/renegmed/nats-patterns-reqreply/usersprovider/pb"

	//"google.golang.org/protobuf/proto"
	"github.com/golang/protobuf/proto"
)

var users = make(map[string]string)
var nc *nats.Conn

func replyWithUserId(m *nats.Msg) {

	myUser := pb.User{}
	err := proto.Unmarshal(m.Data, &myUser)
	if err != nil {
		log.Println("Problem unmarshalling user data.", err)
		return
	}

	myUser.Name = users[myUser.Id]

	log.Println("+++ User:", myUser)

	data, err := proto.Marshal(&myUser)
	if err != nil {
		log.Println("Error on marshalling user", err)
		return
	}

	log.Printf("Replying to %v\n%v\n", m.Reply, myUser)
	//nc.Publish(m.Reply, data)
	m.Respond(data)
}

func main() {

	users["1"] = "Bob"
	users["2"] = "John"
	users["3"] = "Dan"
	users["4"] = "Kate"

	subject := os.Getenv("SUBJECT")
	clientID := os.Getenv("CLIENT_ID")
	natsServers := os.Getenv("NATS_SERVER_ADDR")
	serverPort := os.Getenv("SERVER_PORT")

	log.Printf("Subject: %s, Client ID: %s, natsServers: %s, serverPort: %s", subject, clientID, natsServers, serverPort)

	connector := NewConnector(clientID)

	// Set infinite retries to never stop reconnecting
	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal(err)
	}

	nc := connector.NATS()

	nc.QueueSubscribe(subject, clientID, replyWithUserId)
	// this would provided endless block, useful if this is not a web service
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()

	// Serve HTTP
	r := mux.NewRouter()
	log.Printf("Starting HTTP server on '%s'", serverPort)

	if err := http.ListenAndServe(":"+serverPort, r); err != nil {
		log.Fatal(err)
	}
}
