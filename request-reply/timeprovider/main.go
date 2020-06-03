package main

import (
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/mux"
	nats "github.com/nats-io/nats.go"

	//"google.golang.org/protobuf/proto"
	"github.com/golang/protobuf/proto"
	"github.com/renegmed/nats-patterns-reqreply/timeprovider/pb"
)

var users = make(map[string]string)
var nc *nats.Conn

func replyWithTime(m *nats.Msg) {

	time := time.Now().Format(time.RFC3339)

	curTime := pb.Time{Time: time}
	data, err := proto.Marshal(&curTime)
	if err != nil {
		log.Println("Problem marshalling time data", err)
		return
	}

	log.Printf("Replying to %v\n%v\n", m.Reply, curTime)
	nc.Publish(m.Reply, data)
}

func main() {

	subject := os.Getenv("SUBJECT")
	clientID := os.Getenv("CLIENT_NAME")
	natsServers := os.Getenv("NATS_SERVER_ADDR")
	serverPort := os.Getenv("SERVER_PORT")

	log.Printf("Subject: %s, Client ID: %s, natsServers: %s, serverPort: %s", subject, clientID, natsServers, serverPort)

	connector := NewConnector(clientID)

	// Set infinite retries to never stop reconnecting
	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal("Problem setting up connection to NATS servers", err)
	}

	nc := connector.NATS()

	nc.QueueSubscribe(subject, clientID, replyWithTime)
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
