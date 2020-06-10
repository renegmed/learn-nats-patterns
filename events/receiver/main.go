package main

import (
	"log"
	"os"
	"strings"

	nats "github.com/nats-io/nats.go"
	"github.com/renegmed/nats-patterns-events/receiver/pb"
)

func main() {

	clientID := os.Getenv("CLIENT_ID")
	natsServers := os.Getenv("NATS_SERVER_ADDR")
	serverPort := os.Getenv("SERVER_PORT")

	log.Printf("Client ID: %s, natsServers: %s, serverPort: %s", clientID, natsServers, serverPort)

	connector := NewConnector(clientID)

	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal("", err)
	}

	nc := connector.NATS()

	// automatically encode our structs into raw data. We’ll use the protobuff one,
	// but there are a default one, a gob one and a JSON one too
	//    natsp "github.com/nats-io/nats.go/encoders/protobuf"
	// ec, err := nats.NewEncodedConn(nc, natsp.PROTOBUF_ENCODER)
	ec, err := nats.NewEncodedConn(nc, "json")
	if err != nil {
		log.Fatal("Error on creating encoded connection, ", err)
	}
	defer ec.Close()

	ec.Subscribe("Messaging.Text.Standard", func(m *pb.TextMessage) {
		log.Println("^^^^^^^ Got standard message: \"", m.Body, "\" with the Id ", m.Id, ".")
	})

	ec.Subscribe("Messaging.Text.Respond", func(subject, reply string, m *pb.TextMessage) {
		log.Println("::::::: Got ask for response message: \"", m.Body, "\" with the Id ", m.Id, ".")

		newMessage := pb.TextMessage{Id: m.Id, Body: "Responding to question - " + strings.TrimSuffix(m.Body, " Please respond!")}
		ec.Publish(reply, &newMessage)

	})

	receiveChannel := make(chan *pb.TextMessage)
	ec.BindRecvChan("Messaging.Text.Channel", receiveChannel)

	for m := range receiveChannel {
		log.Println("....Got channeled message: \"", m.Body, "\" with the Id ", m.Id, ".")
	}

	log.Println("End of the application.")
}
