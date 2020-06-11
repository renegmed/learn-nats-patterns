package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
)

const indexPage = "<html><head><title>Upload file</title></head><body><form enctype=\"multipart/form-data\" action=\"submitTask\" method=\"post\"> <input type=\"file\" name=\"uploadfile\" /> <input type=\"submit\" value=\"upload\" /> </form> </body> </html>"

var keyValueStoreAddress string
var masterLocation string

func main() {
	serverPort := os.Getenv("SERVER_PORT")
	keyValueStoreAddress := os.Getenv("KV_STORE_ADDRESS")

	response, err := http.Get("http://" + keyValueStoreAddress + "/get?key=masterAddress")
	if response.StatusCode != http.StatusOK {
		fmt.Println("Error: can't get master address.")
		fmt.Println(response.Body)
		return
	}
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	masterLocation = string(data)
	if len(masterLocation) == 0 {
		fmt.Println("Error: can't get master address. Length is zero.")
		return
	}

	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/submitTask", handleTask)
	http.HandleFunc("/isReady", handleCheckForReadiness)
	http.HandleFunc("/getImage", serveImage)

	log.Printf("Frontend server start on port: %s ....\n", serverPort)
	http.ListenAndServe(":"+serverPort, nil)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, indexPage)
}

func handleTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		err := r.ParseMultipartForm(10000000)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}
		file, _, err := r.FormFile("uploadfile")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}

		response, err := http.Post("http://"+masterLocation+"/new", "image", file)
		file.Close()
		if err != nil || response.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		data, err := ioutil.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		fmt.Fprint(w, string(data))
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only POST accepted")
	}
}

func handleCheckForReadiness(w http.ResponseWriter, r *http.Request) {
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

		response, err := http.Get("http://" + masterLocation + "/isReady?id=" + values.Get("id") + "&state=finished")
		if err != nil || response.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		data, err := ioutil.ReadAll(response.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		switch string(data) {
		case "0":
			fmt.Fprint(w, "Your image is not ready yet.")
		case "1":
			fmt.Fprint(w, "Your image is ready.")
		default:
			fmt.Fprint(w, "Internal server error.")
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only GET accepted")
	}
}

func serveImage(w http.ResponseWriter, r *http.Request) {
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

		response, err := http.Get("http://" + masterLocation + "/get?id=" + values.Get("id") + "&state=finished")
		if err != nil || response.StatusCode != http.StatusOK {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		_, err = io.Copy(w, response.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error: Only GET accepted")
	}
}
