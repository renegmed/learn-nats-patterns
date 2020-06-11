package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
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

		log.Println("I am @fileserver GET ")

		vars := mux.Vars(r)
		file, err := os.Open("/tmp/" + vars["name"])
		defer file.Close()
		if err != nil {
			w.WriteHeader(404)
			http.Error(w, fmt.Sprintf("Error while accessing file. %v\n", err), 404)

		}
		if file != nil {

			_, err := io.Copy(w, file) // Send file copy to requester

			if err != nil {
				http.Error(w, fmt.Sprintf("Error while copying file. %v\n", err), 500)
			}
		}
	}).Methods("GET")

	m.HandleFunc("/{name}", func(w http.ResponseWriter, r *http.Request) {

		vars := mux.Vars(r)

		file, err := os.Create("/tmp/" + vars["name"])
		if err != nil {
			log.Println("Error while creating file")
			http.Error(w, fmt.Sprintf("Error while creating file %s. %v\n", vars["name"], err), 500)
			return
		}

		defer file.Close()

		if file != nil {

			var dat []byte
			if r.Body != nil {
				dat, err = ioutil.ReadAll(r.Body)
				if err != nil {
					log.Println("Error while reading r.Body, ", err)
				}
			}
			// Restore the io.ReadCloser to its original state
			r.Body = ioutil.NopCloser(bytes.NewBuffer(dat))

			log.Printf("\n   Content received from source - \n\tFile name: %s\n\tData:%s\n", vars["name"], string(dat))

			_, err := io.Copy(file, r.Body)
			if err != nil {
				http.Error(w, fmt.Sprintf("Error while copying file %s. %v\n", vars["name"], err), 500)
				return
			}

			/*
				Example file created on fileserver $cat /tmp/cat 83111c2e-0e9d-4559-8649-1f41526eba64 (from worker)
					my : 2
					data : 1
					get : 3
					have : 1

				OR

				Example on fileserver $cat /tmp/cat 83111c2e-0e9d-4559-8649-1f41526eba64 (from master)
					get,my,data,my,get,get,have
			*/
		}

	}).Methods("POST")

	RunServiceDiscoverable()

	http.ListenAndServe(":"+serverPort, m)
}

//
// connects to the NATS server and responds with its own http address to incoming requests
//
func RunServiceDiscoverable() {

	natsServers := os.Getenv("NATS_SERVER_ADDR")
	clientID := os.Getenv("CLIENT_ID")
	transportURL := os.Getenv("TRANSPORT_URL")

	connector := NewConnector(clientID)

	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Print("Problem setting up connection to NATS servers", err)
		return
	}

	nc := connector.NATS()

	log.Printf("natsServers: %s, clientID: %s, transportURL: %s\n", natsServers, clientID, transportURL)

	nc.Subscribe("Discovery.FileServer", func(m *nats.Msg) {
		log.Println("i am in @ Discovery.Fileserver")

		serviceAddressTransport := pb.DiscoverableServiceTransport{Address: transportURL} //"http://fileserver:4200"}

		data, err := proto.Marshal(&serviceAddressTransport)
		if err == nil {
			m.Respond(data)
		} else {
			log.Println("Error while unmarshalling service address transport", err)
		}
	})
}
