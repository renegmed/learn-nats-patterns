package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	Id    int `json:"id"`
	State int `json:"state"`
}

var datastore map[int]Task
var datastoreMutex sync.RWMutex
var oldestNotFinishedTask int // remember to account for potential int overflow in production. Use something bigger.
var oNFTMutex sync.RWMutex

func main() {

	serverPort := os.Getenv("SERVER_PORT")
	keyValueStoreAddress := os.Getenv("KV_STORE_ADDRESS")
	databaseAddress := os.Getenv("DATABASE_ADDRESS")

	if !registerInKVStore(databaseAddress, keyValueStoreAddress) {
		return
	}

	log.Println("serverPort: %s, key-value store addr: %s, database addr: %s", serverPort, keyValueStoreAddress, databaseAddress)

	datastore = make(map[int]Task)
	datastoreMutex = sync.RWMutex{}
	oldestNotFinishedTask = 0
	oNFTMutex = sync.RWMutex{}

	http.HandleFunc("/getById", getById)
	http.HandleFunc("/newTask", newTask)
	http.HandleFunc("/getNewTask", getNewTask)
	http.HandleFunc("/finishTask", finishTask)
	http.HandleFunc("/setById", setById)
	http.HandleFunc("/list", list)
	log.Printf("Started database server port %s .....\n", serverPort)
	http.ListenAndServe(":"+serverPort, nil)
}

func getById(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			fmt.Fprint(w, err)
			return
		}
		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}

		id, err := strconv.Atoi(string(values.Get("id")))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		datastoreMutex.RLock()
		bIsInError := err != nil || id >= len(datastore) // Reading the length of a slice must be done in a synchronized manner. That's why the mutex is used.
		datastoreMutex.RUnlock()

		if bIsInError {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}

		datastoreMutex.RLock()
		value := datastore[id]
		datastoreMutex.RUnlock()

		response, err := json.Marshal(value)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		fmt.Fprint(w, string(response))
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only GET accepted")
	}
}

func newTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		datastoreMutex.Lock()
		taskToAdd := Task{
			Id:    len(datastore),
			State: 0,
		}
		datastore[taskToAdd.Id] = taskToAdd
		datastoreMutex.Unlock()

		fmt.Fprint(w, taskToAdd.Id)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func getNewTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {

		bErrored := false

		datastoreMutex.RLock()
		if len(datastore) == 0 {
			bErrored = true
		}
		datastoreMutex.RUnlock()

		if bErrored {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: No non-started task.")
			return
		}

		taskToSend := Task{Id: -1, State: 0}

		oNFTMutex.Lock()
		datastoreMutex.Lock()
		for i := oldestNotFinishedTask; i < len(datastore); i++ {
			if datastore[i].State == 2 && i == oldestNotFinishedTask {
				oldestNotFinishedTask++
				continue
			}
			if datastore[i].State == 0 {
				datastore[i] = Task{Id: i, State: 1}
				taskToSend = datastore[i]
				break
			}
		}
		datastoreMutex.Unlock()
		oNFTMutex.Unlock()

		if taskToSend.Id == -1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: No non-started task.")
			return
		}

		myId := taskToSend.Id

		go func() {
			time.Sleep(time.Second * 120)
			datastoreMutex.Lock()
			if datastore[myId].State == 1 {
				datastore[myId] = Task{Id: myId, State: 0}
			}
			datastoreMutex.Unlock()
		}()

		response, err := json.Marshal(taskToSend)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		fmt.Fprint(w, string(response))
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func finishTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			fmt.Fprint(w, err)
			return
		}
		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}

		id, err := strconv.Atoi(string(values.Get("id")))

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		updatedTask := Task{Id: id, State: 2}

		bErrored := false

		datastoreMutex.Lock()
		if datastore[id].State == 1 {
			datastore[id] = updatedTask
		} else {
			bErrored = true
		}
		datastoreMutex.Unlock()

		if bErrored {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input")
			return
		}

		fmt.Fprint(w, "success")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func setById(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		taskToSet := Task{}

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}
		err = json.Unmarshal([]byte(data), &taskToSet)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		bErrored := false
		datastoreMutex.Lock()
		if taskToSet.Id >= len(datastore) || taskToSet.State > 2 || taskToSet.State < 0 {
			bErrored = true
		} else {
			datastore[taskToSet.Id] = taskToSet
		}
		datastoreMutex.Unlock()

		if bErrored {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input")
			return
		}

		fmt.Fprint(w, "success")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func list(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		datastoreMutex.RLock()
		for key, value := range datastore {
			fmt.Fprintln(w, key, ": ", "id:", value.Id, " state:", value.State)
		}
		datastoreMutex.RUnlock()
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only GET accepted")
	}
}

func registerInKVStore(databaseAddress, keyValueStoreAddress string) bool {
	response, err := http.Post("http://"+keyValueStoreAddress+"/set?key=databaseAddress&value="+databaseAddress, "", nil)
	if err != nil {
		fmt.Println("Error on post key-value store,", err)
		return false
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println("Error, unable to read key-value store response body,", err)
		return false
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: Failure when contacting key-value store: ", string(data))
		return false
	}
	return true
}
