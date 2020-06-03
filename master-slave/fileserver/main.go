package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/gorilla/mux"
	nats "github.com/nats-io/nats.go"
	"github.com/renegmed/nats-pattern-masterslave/fileserver/pb"
)

func main() {

	serverPort := os.Getenv("SERVER_PORT")

	log.Printf("serverPort: %s", serverPort)

	m := mux.NewRouter()

	m.HandleFunc("/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		file, err := os.Open("/tmp/" + vars["name"])
		defer file.Close()
		if err != nil {
			w.WriteHeader(404)
			http.Error(w, fmt.Sprintf("Error while accessing file. %v\n", err), 404)

		}
		if file != nil {
			_, err := io.Copy(w, file)
			if err != nil {
				//w.WriteHeader(500)
				http.Error(w, fmt.Sprintf("Error while copying file. %v\n", err), 500)
			}
		}
	}).Methods("GET")

	m.HandleFunc("/{name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		file, err := os.Create("/tmp/" + vars["name"])
		defer file.Close()
		if err != nil {
			//w.WriteHeader(500)
			http.Error(w, fmt.Sprintf("Error while creating file %s. %v\n", vars["name"], err), 500)
		}
		if file != nil {
			_, err := io.Copy(file, r.Body)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error while copying file %s. %v\n", vars["name"], err), 500)
				//w.WriteHeader(500)
			}
		}
	}).Methods("POST")

	RunServiceDiscoverable()

	http.ListenAndServe(":"+serverPort, m)
}

func RunServiceDiscoverable() {

	natsServers := os.Getenv("NATS_SERVER_ADDR")
	clientID := os.Getenv("CLIENT_ID")
	transportURL := os.Getenv("TRANSPORT_URL")

	log.Printf("natsServers: %s, clientID: %s, transportURL: %s\n", natsServers, clientID, transportURL)

	connector := NewConnector(clientID)

	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Print("Problem setting up connection to NATS servers", err)
		return
	}

	nc := connector.NATS()

	nc.Subscribe("Discovery.FileServer", func(m *nats.Msg) {
		serviceAddressTransport := pb.DiscoverableServiceTransport{Address: transportURL} //"http://localhost:3000"}
		data, err := proto.Marshal(&serviceAddressTransport)
		if err == nil {
			nc.Publish(m.Reply, data)
		}
	})
}
