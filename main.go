package main

import (
	"bufio"
	"code.sajari.com/docconv"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"time"
)

type document struct {
	UId        uuid.UUID `json:"uid"`
	Name       string    `json:"name"`
	TimeUpload time.Time `json:"uploaded_at"`
}

type word struct {
	UId         uuid.UUID `json:"uid"`
	IdParagraph int64     `json:"paragraph"`
	IdSentence  int64     `json:"sentence"`
	Word        string    `json:"word"`
	IdTag       int64     `json:"tag"`
}

type wordU struct {
	IdParagraph int64  `json:"paragraph"`
	IdSentence  int64  `json:"sentence"`
	Word        string `json:"word"`
	IdTag       int64  `json:"tag"`
}

type allDocuments []document
type Text []word
type TextU []wordU

var documents = allDocuments{
	{
		UId:        uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		Name:       "Test",
		TimeUpload: time.Now(),
	},
	{
		UId:        uuid.MustParse("122a3c40-5d3b-217e-707b-6a546a3d7b29"),
		Name:       "Test1",
		TimeUpload: time.Now(),
	},
}

var texts = Text{
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "Я",
		IdTag:       1,
	},
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "тебя",
		IdTag:       3,
	},
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "люблю",
		IdTag:       2,
	},
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        ".",
		IdTag:       0,
	},
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  2,
		Word:        "А",
		IdTag:       0,
	},
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  2,
		Word:        "я",
		IdTag:       1,
	},
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  2,
		Word:        "тебя",
		IdTag:       3,
	},
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  2,
		Word:        "нет",
		IdTag:       2,
	},
	{
		UId:         uuid.MustParse("612f3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  2,
		Word:        ".",
		IdTag:       0,
	},
	{
		UId:         uuid.MustParse("122a3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "Этот",
		IdTag:       4,
	},
	{
		UId:         uuid.MustParse("122a3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "текст",
		IdTag:       1,
	},
	{
		UId:         uuid.MustParse("122a3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "ни",
		IdTag:       0,
	},
	{
		UId:         uuid.MustParse("122a3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "о",
		IdTag:       0,
	},
	{
		UId:         uuid.MustParse("122a3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "чем",
		IdTag:       3,
	},
	{
		UId:         uuid.MustParse("122a3c40-5d3b-217e-707b-6a546a3d7b29"),
		IdParagraph: 1,
		IdSentence:  1,
		Word:        "?",
		IdTag:       0,
	},
}

func homePage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(document{UId: uuid.New(), Name: "Hello world", TimeUpload: time.Now()})
}

func uploadDocument(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(1000000)
	file, handler, err := r.FormFile("file")
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
	documents = append(documents, document{UId: uuid.New(), Name: handler.Filename, TimeUpload: time.Now()})
	json.NewEncoder(w).Encode(resp)
}

func getDocuments(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(documents)
}

func getDocText(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	resp := TextU{}
	for _, item := range texts {
		if item.UId == uuid.MustParse(params["id"]) {
			resp = append(resp, wordU{IdParagraph: item.IdParagraph, IdSentence: item.IdSentence, Word: item.Word, IdTag: item.IdTag})
		}
	}
	json.NewEncoder(w).Encode(resp)
}

func getStatisticWord(w http.ResponseWriter, r *http.Request) {

}

func main() {
	router := mux.NewRouter().StrictSlash(true)

	router.HandleFunc("/", homePage)
	router.HandleFunc("/documents", uploadDocument).Methods("POST")
	router.HandleFunc("/documents", getDocuments).Methods("GET")
	router.HandleFunc("/documents/{id}/text", getDocText).Methods("GET")
	//router.HandleFunc("/documents/{id}/{word}", getStatisticWord).Methods("GET")

	log.Fatal(http.ListenAndServe(":8000", router))
}
