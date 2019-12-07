package main

import (
	"bufio"
	"code.sajari.com/docconv"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

type document struct {
	Name string `json:"text"`
	Byte []byte `json:"byte"`
}
type allDocuments []document

var documents = allDocuments{
	{
		Name: "Test",
		Byte: []byte("Это тестовое предложение. Ты назвал его рыбой. "),
	},
	{
		Name: "Test2",
		Byte: []byte("Это второе тестовое предложение. "),
	},
}

func homePage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(document{Name: "Hello world", Byte: []byte("Hello world")})
}

func uploadDocument(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)
	r.ParseMultipartForm(1000000)
	file, handler, err := r.FormFile("uploadfile")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	resp, _, err := docconv.ConvertDocx(reader)
	if err != nil {
		fmt.Errorf("got error = %v, want nil", err)
	}
	documents = append(documents, document{Name: handler.Filename, Byte: []byte(resp)})
}

func getDocuments(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(documents)
}

func getDocText(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	for _, item := range documents {
		if item.Name == params["id"] {
			json.NewEncoder(w).Encode(item)
			return
		}
	}
	json.NewEncoder(w).Encode(&document{})

}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", homePage)
	router.HandleFunc("/upload", uploadDocument).Methods("POST")
	router.HandleFunc("/documents", getDocuments).Methods("GET")
	router.HandleFunc("/documents/{id}/text", getDocText).Methods("GET")

	log.Fatal(http.ListenAndServe(":8000", router))
}
