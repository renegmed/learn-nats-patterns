package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
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

	// automatically encode our structs into raw data. We’ll use the protobuff one,
	// but there are a default one, a gob one and a JSON one too
	// 	natsp "github.com/nats-io/nats.go/encoders/protobuf"
	//ec, err := nats.NewEncodedConn(nc, natsp.PROTOBUF_ENCODER) // "protobuf"
	ec, err := nats.NewEncodedConn(nc, "json")
	defer ec.Close()

	messages := []string{"Hello there!", "How are you?", "Anything new?", "How are you doing?", "What's up"}

	var wg = sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			myMessage := pb.TextMessage{Id: int32(i), Body: messages[i]} //"Hello over standard!"}

			//log.Println("++++++ myMessage:\n", myMessage)

			err := ec.Publish("Messaging.Text.Standard", &myMessage)
			if err != nil {
				log.Println("Error on publishing messasging text standard, ", err)
			}
		}(i)
	}

	for i := 5; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			myMessage := pb.TextMessage{Id: int32(i), Body: messages[i-5] + " Please respond!"} //"Hello, please respond!"}

			res := pb.TextMessage{}
			err := ec.Request("Messaging.Text.Respond", &myMessage, &res, 4000*time.Millisecond)
			if err != nil {
				log.Println("Error on requesting messaging text respond, ", err)
			}

			fmt.Println(res.Body, " with id ", res.Id)
		}(i)
	}

	wg.Wait()

	sendChannel := make(chan *pb.TextMessage)
	ec.BindSendChan("Messaging.Text.Channel", sendChannel)
	for i := 10; i < 15; i++ {
		myMessage := pb.TextMessage{Id: int32(i), Body: "Hello over channel!"}

		sendChannel <- &myMessage
	}

	log.Println("End of the application.")
}
