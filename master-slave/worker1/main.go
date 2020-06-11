package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/nats.go"
	"github.com/renegmed/nats-pattern-masterslave/worker/pb"
	uuid "github.com/satori/go.uuid"
)

var nc *nats.Conn

func main() {
	natsServers := os.Getenv("NATS_SERVER_ADDR")
	serverPort := os.Getenv("SERVER_PORT")
	clientID := os.Getenv("CLIENT_ID")

	connector := NewConnector(clientID)
	// Set infinite retries to never stop reconnecting
	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal("Problem setting up connection to NATS servers", err)
	}

	nc = connector.NATS()

	log.Printf("Client ID: %s, natsServers: %s, serverPort: %s", clientID, natsServers, serverPort)

	for i := 0; i < 8; i++ {
		go doWork()
	}

	select {}
}

func doWork() {
	for {

		time.Sleep(3 * time.Second)

		// We ask for a Task with a 1 second Timeout
		msg, err := nc.Request("Work.TaskToDo", nil, 5*time.Second)
		if err != nil {
			log.Println("No new task...") // sent. Waiting 5 seconds before retrying:", err)
			continue
		}

		// We unmarshal the Task
		curTask := pb.Task{}
		err = proto.Unmarshal(msg.Data, &curTask)
		if err != nil {
			log.Println("Problem on unmarshalling received data. Waiting 2 seconds before retrying:", err)
			continue
		}

		log.Printf("\n   Assigned task received from master -\n\tUuid: %s\n\tFinished Uuid: %s\n\tState: %d\n\tId: %d\n",
			curTask.Uuid, curTask.Finisheduuid, curTask.State, curTask.Id)

		// We get the FileServer address
		msg, err = nc.Request("Discovery.FileServer", nil, 1000*time.Millisecond)
		if err != nil {
			log.Println("Problem on requesting discovery fileserver. Waiting 2 seconds before retrying:", err)
			continue
		}

		fileServerAddressTransport := pb.DiscoverableServiceTransport{}
		err = proto.Unmarshal(msg.Data, &fileServerAddressTransport)
		if err != nil {
			log.Println("Problem on unmarshalling address transport data. Waiting 2 seconds before retrying:", err)
			continue
		}

		// We get the file
		fileServerAddress := fileServerAddressTransport.Address

		//log.Println("Task from file server: ", fileServerAddress+"/"+curTask.Uuid)

		r, err := http.Get(fileServerAddress + "/" + curTask.Uuid)
		if err != nil {
			log.Printf("Problem on starting http Get task uuid %s. Waiting 2 seconds before retrying: %v\n", curTask.Uuid, err)
			continue
		}

		data, err := bodyBytes(r)
		if err != nil {
			log.Println("Problem on reading http message. Waiting 2 seconds before retrying:", err)
			continue
		}

		log.Printf("\n   Received data from fileserver task id %s -\n\t%s", curTask.Uuid, string(data))

		// We split and count the words
		words := strings.Split(string(data), ",")
		sort.Strings(words)
		wordCounts := make(map[string]int)
		for i := 0; i < len(words); i++ {
			wordCounts[words[i]] = wordCounts[words[i]] + 1
		}

		resultData := make([]byte, 0, 1024)
		buf := bytes.NewBuffer(resultData)

		// We print the results to a buffer
		for key, value := range wordCounts {
			fmt.Fprintln(buf, key, ":", value)
		}

		uid := uuid.NewV4()

		curTask.Finisheduuid = uid.String()

		b, err := bodyBytes(r)
		if err != nil {
			println("Error while reading buffer, ", err)
		}

		r, err = http.Post(fileServerAddress+"/"+curTask.Finisheduuid, "", buf)

		if err != nil || r.StatusCode != http.StatusOK {
			log.Printf("Problem on http post task finished: %s. \n\tWaiting 2 seconds before retrying:%v\n\tstatus code: %v\n",
				curTask.Finisheduuid, err, ":", r.StatusCode)
			continue
		}

		log.Printf("\n   Posted finished task data (word counts) to fileserver - \n\tUuid: %s\n\tcontent: %s\n", curTask.Finisheduuid, string(b))

		// We marshal the current Task into a protobuffer
		data, err = proto.Marshal(&curTask)
		if err != nil {
			fmt.Println("Problem on marshalling task data. Waiting 2 seconds before retrying:", err)
			continue
		}

		// We notify the Master about finishing the Task
		log.Printf("\n   Notifying master finished task -\n\tUuid: %s\n\tFinished Uuid: %s\n\tState: %d\n\tId: %d\n",
			curTask.Uuid, curTask.Finisheduuid, curTask.State, curTask.Id)

		nc.Publish("Work.TaskFinished", data)
	}
}

func bodyBytes(r *http.Response) ([]byte, error) {
	if r.Body != nil {
		dat, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, fmt.Errorf("Error while reading r.Body,  %v", err)
		}
		r.Body = ioutil.NopCloser(bytes.NewBuffer(dat))
		return dat, nil
	}
	return []byte{}, nil
}
