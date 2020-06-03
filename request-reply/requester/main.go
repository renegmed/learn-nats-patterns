package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/mux"
	nats "github.com/nats-io/nats.go"
	"github.com/renegmed/nats-patterns-reqreply/requester/pb"

	"github.com/golang/protobuf/proto"
)

var nc *nats.Conn

func handleUserWithTime(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	myUser := pb.User{Id: vars["id"]}
	curTime := pb.Time{}
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		data, err := proto.Marshal(&myUser)
		if err != nil || len(myUser.Id) == 0 {
			log.Println("Problem with parsing the user Id.", err)
			//w.WriteHeader(500)
			http.Error(w, fmt.Sprintf("Problem marshalling user data. %v", err), 500)
			return
		}

		log.Println("Requesting user userNameById data,", myUser)

		msg, err := nc.Request("UserNameById", data, 2000*time.Millisecond)
		if err != nil {
			log.Println("Problem requesting user data.", err)
			//w.WriteHeader(500)
			http.Error(w, fmt.Sprintf("Problem requesting user data. %v", err), 500)
			return
		}

		if msg != nil {
			log.Println("Problem, no message from user data provider.")
			//w.WriteHeader(500)
			http.Error(w, fmt.Sprintf("Problem, no message from user data provider. %v", err), 500)
			return
		}
		myUserWithName := pb.User{}
		err = proto.Unmarshal(msg.Data, &myUserWithName)
		if err != nil {
			log.Println("Problem unmarshalling user data.", err)
			//w.WriteHeader(500)
			http.Error(w, fmt.Sprintf("Problem unmarshalling user data. %v", err), 500)
			return
		}
		myUser = myUserWithName
		log.Println("Got from data provider, ", myUser)

	}()

	go func() {
		defer wg.Done()
		msg, err := nc.Request("TimeTeller", nil, 2000*time.Millisecond)
		if err != nil {
			log.Println("Problem requesting time teller data.", err)
			http.Error(w, fmt.Sprintf("Problem on requesting time teller data.", err), 500)
			return
		}

		if msg == nil {
			log.Println("Problem, no message from time teller.")
			//w.WriteHeader(500)
			http.Error(w, fmt.Sprintf("Problem, no message from time teller.", err), 500)
			return
		}

		receivedTime := pb.Time{}
		err = proto.Unmarshal(msg.Data, &receivedTime)
		if err != nil {
			log.Println("Problem unmarshalling time teller data.", err)
			//w.WriteHeader(500)
			http.Error(w, fmt.Sprintf("Problem unmarshalling time teller data.", err), 500)
			return
		}
		curTime = receivedTime
		log.Println("Got from time teller, ", curTime)
	}()

	wg.Wait()

	fmt.Fprintln(w, "Hello ", myUser.Name, " with id ", myUser.Id, ", the time is ", curTime.Time, ".")

}

func main() {

	//subject := os.Getenv("SUBJECT")
	clientID := os.Getenv("CLIENT_ID")
	natsServers := os.Getenv("NATS_SERVER_ADDR")
	serverPort := os.Getenv("SERVER_PORT")

	log.Printf("Client ID: %s, natsServers: %s, serverPort: %s", clientID, natsServers, serverPort)

	connector := NewConnector(clientID)

	// Set infinite retries to never stop reconnecting
	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal(err)
	}

	nc = connector.NATS()

	// Serve HTTP
	r := mux.NewRouter()
	r.HandleFunc("/{id}", handleUserWithTime)
	log.Printf("Starting HTTP server on '%s'", serverPort)

	if err := http.ListenAndServe(":"+serverPort, r); err != nil {
		log.Fatal(err)
	}
}
