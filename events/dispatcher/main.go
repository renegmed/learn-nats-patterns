package main

import (
	"fmt"
	"log"
	"os"
	"time"

	nats "github.com/nats-io/nats.go"
	natsp "github.com/nats-io/nats.go/encoders/protobuf"
	"github.com/renegmed/nats-patterns-events/dispatcher/pb"
)

func main() {

	clientID := os.Getenv("CLIENT_ID")
	natsServers := os.Getenv("NATS_SERVER_ADDR")
	serverPort := os.Getenv("SERVER_PORT")

	log.Printf("Client ID: %s, natsServers: %s, serverPort: %s", clientID, natsServers, serverPort)

	connector := NewConnector(clientID)

	// Set infinite retries to never stop reconnecting
	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal("Error on setting up nats connection,", err)
	}

	nc := connector.NATS()

	ec, err := nats.NewEncodedConn(nc, natsp.PROTOBUF_ENCODER)
	defer ec.Close()

	for i := 0; i < 5; i++ {
		myMessage := pb.TextMessage{Id: int32(i), Body: "Hello over standard!"}

		err := ec.Publish("Messaging.Text.Standard", &myMessage)
		if err != nil {
			log.Println("Error on publishing messasging text standard, ", err)
		}
	}

	for i := 5; i < 10; i++ {
		myMessage := pb.TextMessage{Id: int32(i), Body: "Hello, please respond!"}

		res := pb.TextMessage{}
		err := ec.Request("Messaging.Text.Respond", &myMessage, &res, 200*time.Millisecond)
		if err != nil {
			log.Println("Error on requesting messaging text respond, ", err)
		}

		fmt.Println(res.Body, " with id ", res.Id)

	}

	sendChannel := make(chan *pb.TextMessage)
	ec.BindSendChan("Messaging.Text.Channel", sendChannel)
	for i := 10; i < 15; i++ {
		myMessage := pb.TextMessage{Id: int32(i), Body: "Hello over channel!"}

		sendChannel <- &myMessage
	}

	log.Println("End of the application.")
}
