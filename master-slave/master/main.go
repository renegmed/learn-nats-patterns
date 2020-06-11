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

func main() {
	clientID := os.Getenv("CLIENT_ID")
	natsServers := os.Getenv("NATS_SERVER_ADDR")
	serverPort := os.Getenv("SERVER_PORT")

	connector := NewConnector(clientID)

	// Set infinite retries to never stop reconnecting
	err := connector.SetupConnectionToNATS(natsServers, nats.MaxReconnects(-1))
	if err != nil {
		log.Fatal("Problem setting up connection to NATS servers", err)
	}

	nc := connector.NATS()

	log.Printf("Client ID: %s, natsServers: %s, serverPort: %s", clientID, natsServers, serverPort)

	Tasks = make([]pb.Task, 0, 20)
	TaskMutex = sync.Mutex{}
	oldestFinishedTaskPointer = 0

	initTestTasks(nc)

	nc.Subscribe("Work.TaskToDo", func(m *nats.Msg) {

		//log.Println("i am in @ Work.TaskToDo....")

		myTaskPointer, ok := getNextTask()
		if ok {

			data, err := proto.Marshal(myTaskPointer)
			if err == nil {

				log.Printf("\n   Sending next task data to worker -\n\tUuid: %s\n\tFinished Uuid: %s\n\tState: %d\n\tId: %d\n",
					myTaskPointer.Uuid, myTaskPointer.Finisheduuid, myTaskPointer.State, myTaskPointer.Id)

				m.Respond(data)
			} else {
				log.Println("Problem while marshalling next task data")
			}
		} else {
			log.Println("No Task available.")
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

			log.Printf("\n   Received finished task data from worker - \n\tUuid: %s\n\tFinished Uuid: %s\n\tState: %d\n\tId: %d\n",
				Tasks[myTask.Id].Uuid, Tasks[myTask.Id].Finisheduuid, Tasks[myTask.Id].State, Tasks[myTask.Id].Id)

		} else {
			log.Println("Problem while unmarshalling finished task data")
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

func initTestTasks(nc *nats.Conn) {

	dataToProcess := []string{
		"give,thanks,vowel,happy,get,thanks,happy,thanks,happy,thanks,give",
		"terse,word,terse,sad,choice,correct,word,word,work,terse,ties,correct,correct",
		"the,their,his,her,them,out,their,her,it,their,their,the,is,is,the,the,her,her",
		"grapple,who,is,she,grapple,who,is,is,is,been,benn,who,she,she,been,who,show",
		"asked,repeat,recent,try,what,asked,asked,repeat,repeat,repeat,recent,repeat,try,what,what,what,repeat,repeat",
	}

	for i := 0; i < len(dataToProcess); i++ {

		time.Sleep(1000 * time.Millisecond)
		uid := uuid.NewV4()

		newTask := pb.Task{Uuid: uid.String(), State: 0}
		fileServerAddressTransport := pb.DiscoverableServiceTransport{}
		msg, err := nc.Request("Discovery.FileServer", nil, 1000*time.Millisecond)
		if err == nil && msg != nil {

			err := proto.Unmarshal(msg.Data, &fileServerAddressTransport) // this should be fileserver uri http://fileserver:4200
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

		fmt.Fprint(buf, dataToProcess[i]) // "get,my,data,my,get,get,have") // send this data to fileserver and save them to a file

		r, err := http.Post(fileServerAddress+"/"+newTask.Uuid, "", buf)
		if err != nil {
			log.Printf("Not good. Received error for posting request,%v\n", err)
			continue
		}

		if r.StatusCode != http.StatusOK {
			log.Printf("Not good. Received status code %d.", r.StatusCode)
			continue
		}

		newTask.Id = int32(len(Tasks))
		Tasks = append(Tasks, newTask)
	}
}
