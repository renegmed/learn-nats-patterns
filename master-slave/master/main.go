package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	nats "github.com/nats-io/nats.go"
	"github.com/renegmed/nats-pattern-masterslave/master/pb"
	uuid "github.com/satori/go.uuid"
)

var Tasks []pb.Task
var TaskMutex sync.Mutex
var oldestFinishedTaskPointer int
var nc *nats.Conn

func main() {
	clientID := os.Getenv("CLIENT_ID")
	natsServers := os.Getenv("NATS_SERVER_ADDR")
	serverPort := os.Getenv("SERVER_PORT")

	log.Printf("Client ID: %s, natsServers: %s, serverPort: %s", clientID, natsServers, serverPort)

	connector := NewConnector(clientID)

	// Set infinite retries to never stop reconnecting
	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal("Problem setting up connection to NATS servers", err)
	}

	nc := connector.NATS()

	Tasks = make([]pb.Task, 0, 20)
	TaskMutex = sync.Mutex{}
	oldestFinishedTaskPointer = 0

	initTestTasks()

	nc.Subscribe("Work.TaskToDo", func(m *nats.Msg) {
		myTaskPointer, ok := getNextTask()
		if ok {
			data, err := proto.Marshal(myTaskPointer)
			if err == nil {
				nc.Publish(m.Reply, data)
			} else {
				log.Println("Problem while marshalling next task data")
			}
		}
	})

	nc.Subscribe("Work.TaskFinished", func(m *nats.Msg) {
		myTask := pb.Task{}
		err := proto.Unmarshal(m.Data, &myTask)
		if err == nil {
			TaskMutex.Lock()
			Tasks[myTask.Id].State = 2
			Tasks[myTask.Id].Finisheduuid = myTask.Finisheduuid
			TaskMutex.Unlock()
		} else {
			log.Println("Problem while unmarshalling task data")
		}
	})

	select {}

	// http server to keep app alive
	http.ListenAndServe(":"+serverPort, nil)
}

func getNextTask() (*pb.Task, bool) {
	TaskMutex.Lock()
	defer TaskMutex.Unlock()
	for i := oldestFinishedTaskPointer; i < len(Tasks); i++ {
		if i == oldestFinishedTaskPointer && Tasks[i].State == 2 {
			oldestFinishedTaskPointer++
		} else {
			if Tasks[i].State == 0 {
				Tasks[i].State = 1
				go resetTaskIfNotFinished(i)
				return &Tasks[i], true
			}
		}
	}
	return nil, false
}

func resetTaskIfNotFinished(i int) {
	time.Sleep(2 * time.Minute)
	TaskMutex.Lock()
	if Tasks[i].State != 2 {
		Tasks[i].State = 0
	}
}

func initTestTasks() {
	for i := 0; i < 20; i++ {
		uid := uuid.NewV4()
		// if err != nil {
		// 	log.Println("Problem generating task ID,", err)
		// }
		newTask := pb.Task{Uuid: uid.String(), State: 0}
		fileServerAddressTransport := pb.DiscoverableServiceTransport{}
		msg, err := nc.Request("Discovery.FileServer", nil, 1000*time.Millisecond)
		if err == nil && msg != nil {
			err := proto.Unmarshal(msg.Data, &fileServerAddressTransport)
			if err != nil {
				log.Println("Problem unmarshalling fileserver data,", err)
				continue
			}
		}
		if err != nil {
			log.Println("Not good. Received error while requesting discovery fileserver:", err)
			continue
		}

		fileServerAddress := fileServerAddressTransport.Address
		data := make([]byte, 0, 1024)
		buf := bytes.NewBuffer(data)

		fmt.Fprint(buf, "get,my,data,my,get,get,have")

		r, err := http.Post(fileServerAddress+"/"+newTask.Uuid, "", buf)
		if err != nil || r.StatusCode != http.StatusOK {
			log.Println("Not good. Received status code %s or an error of %v\n", r.StatusCode, err)
			continue
		}

		newTask.Id = int32(len(Tasks))
		Tasks = append(Tasks, newTask)
	}
}
