package main

import (
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
)

var keyValueStore map[string]string
var kVStoreMutex sync.RWMutex

func main() {
	serverPort := os.Getenv("SERVER_PORT")

	keyValueStore = make(map[string]string)
	kVStoreMutex = sync.RWMutex{}
	http.HandleFunc("/get", get)
	http.HandleFunc("/set", set)
	http.HandleFunc("/remove", remove)
	http.HandleFunc("/list", list)
	log.Printf("Key-value store server has started with port: %s ....\n", serverPort)
	http.ListenAndServe(":"+serverPort, nil)
}

func get(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error on get parse query:", err)
			return
		}

		// log.Println("Get values:", values)
		// log.Println("Get key:", values.Get("key"))
		// log.Println("Get key len:", len(values.Get("key")))

		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error on get:", "Wrong input key.")
			return
		}

		kVStoreMutex.RLock()
		value := keyValueStore[string(values.Get("key"))]
		kVStoreMutex.RUnlock()

		fmt.Fprint(w, value)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only GET accepted.")
	}
}

func set(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {

		log.Println("Set RawQuery:", r.URL.RawQuery)

		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error on set:", err)
			return
		}

		log.Println("Set values:", values)
		log.Println("Set key:", values.Get("key"))
		log.Println("Set value:", values.Get("value"))

		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error on set:", "Wrong input key.")
			return
		}
		if len(values.Get("value")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error on set:", "Wrong input value.")
			return
		}

		kVStoreMutex.Lock()
		keyValueStore[string(values.Get("key"))] = string(values.Get("value"))
		kVStoreMutex.Unlock()

		fmt.Fprint(w, "success on set")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted.")
	}
}

func remove(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error on remove:", err)
			return
		}
		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error on remove:", "Wrong input key.")
			return
		}

		kVStoreMutex.Lock()
		delete(keyValueStore, values.Get("key"))
		kVStoreMutex.Unlock()

		fmt.Fprint(w, "success on remove")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error on remove : Only DELETE accepted.")
	}
}

func list(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		kVStoreMutex.RLock()
		for key, value := range keyValueStore {
			fmt.Fprintln(w, key, ":", value)
		}
		kVStoreMutex.RUnlock()
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error on list: Only GET accepted.")
	}
}
