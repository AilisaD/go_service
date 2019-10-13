package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"os"
)

type document struct {
	Text string `json:"text"`
	Id   int    `json:"id"`
}
type allDocuments []document

var documents = allDocuments{
	{
		Text: "NameDoc",
		Id: 0,
	},
}

func homePage(w http.ResponseWriter, r *http.Request){
	fmt.Fprintf(w, "first page")
	//w.Header().Set("Content-Type", "application/json")
	//json.NewEncoder(w).Encode(document{Text: "Hello world", Id: 0})
}

func uploadDocument(w http.ResponseWriter, r *http.Request) {
	fmt.Println("method:", r.Method)
	r.ParseMultipartForm(1000000)
	file, handler, err := r.FormFile("uploadfile")
	if err != nil {
		fmt.Println("error")
		fmt.Println(err)
		return
	}
	defer file.Close()
	fmt.Fprintf(w, "%v", handler.Header)


	f, err := os.OpenFile("test/"+handler.Filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer f.Close()
	io.Copy(f, file)
}

func main() {
	router := mux.NewRouter().StrictSlash(true)
	router.HandleFunc("/", homePage)
	router.HandleFunc("/upload", uploadDocument).Methods("POST")
	//router.HandleFunc("/documents/{id}", getDocument).Methods("GET")
	log.Fatal(http.ListenAndServe(":10000", router))
}




