package main

import (
	"bufio"
	"code.sajari.com/docconv"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/adjust/rmq"
	"github.com/go-redis/redis"
	_ "github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strings"
	"time"
)

var CONNECTION = "tcp://clickhouse:8123?username=default&database=default&compress=true&&&alt_hosts=clickhouse:9000,clickhouse:9009"

var db *sql.DB

type document struct {
	UUId       uuid.UUID `json:"document_id"`
	Name       string    `json:"name"`
	TimeUpload time.Time `json:"uploaded_at"`
}

type word struct {
	UUId        uuid.UUID `json:"document_id"`
	IdParagraph int64     `json:"paragraph"`
	IdSentence  int64     `json:"sentence"`
	IdWord      int64     `json:"object"`
	Word        string    `json:"token"`
	IdTag       int64     `json:"tag"`
}

type wordU struct {
	IdParagraph int64  `json:"paragraph"`
	IdSentence  int64  `json:"sentence"`
	Word        string `json:"token"`
	IdTag       int64  `json:"tag"`
}

type allDocuments []document
type Text []word
type TextU []wordU

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func homePage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(document{UUId: uuid.New(), Name: "Hello world", TimeUpload: time.Now()})
}

func uploadDocument(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(1000000)
	file, handler, err := r.FormFile("file")
	if err != nil {
		fmt.Println(err)
		return
	}
	if strings.Contains(handler.Filename, ".docx") {
		uidd := uuid.New()
		timeT := time.Now()
		tx, err := db.Begin()
		checkErr(err)
		qi1, err := tx.Prepare("INSERT INTO document (id, name, time) VALUES (?, ?, ?)")
		checkErr(err)
		if _, err := qi1.Exec(uidd, handler.Filename, timeT); err != nil {
			log.Fatal(err)
		}
		checkErr(tx.Commit())
		client := redis.NewClient(&redis.Options{
			Addr:     "redis:6379",
			Password: "", // no password set
			DB:       1,  // use default DB
		})
		defer file.Close()
		reader := bufio.NewReader(file)
		resp, _, err := docconv.ConvertDocx(reader)
		checkErr(err)
		err = client.Set(fmt.Sprintf("file:%s", uidd.String()), []byte(resp), 0).Err()
		checkErr(err)
		connection := rmq.OpenConnection("producer", "tcp", "redis:6379", 1)
		doc := connection.OpenQueue("documents")
		doc.Publish(uidd.String())
		w.WriteHeader(201)
	}
}

func getDocuments(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if db == nil {
		json.NewEncoder(w).Encode("DB connection lost")
		return
	}
	rows, err := db.Query("SELECT id, name, time FROM document")
	checkErr(err)
	doc := allDocuments{}
	for rows.Next() {
		var (
			id    uuid.UUID
			name  string
			timeT time.Time
		)
		checkErr(rows.Scan(&id, &name, &timeT))
		doc = append(doc, document{UUId: id, Name: name, TimeUpload: timeT})
	}
	json.NewEncoder(w).Encode(doc)
}

func getDocText(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	rows, _ := db.Query(fmt.Sprintf("SELECT id_paragraph, id_sentence, word, tag FROM text WHERE id_document=toUUID('%s') ORDER BY [id_paragraph, id_sentence, id_word]", params["id"]))
	resp := TextU{}
	for rows.Next() {
		var (
			paragraph int64
			sentence  int64
			word      string
			tag       int64
		)
		if err := rows.Scan(&paragraph, &sentence, &word, &tag); err != nil {
			w.WriteHeader(404)
			return
		}
		resp = append(resp, wordU{IdParagraph: paragraph, IdSentence: sentence, Word: word, IdTag: tag})
	}
	json.NewEncoder(w).Encode(resp)

}

func getStatistic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	params := mux.Vars(r)
	rows, err := db.Query(fmt.Sprintf("SELECT COUNT(*) FROM text WHERE word='%s' ORDER BY [id_paragraph, id_sentence, id_word]", params["token"]))
	checkErr(err)
	resp := TextU{}
	for rows.Next() {
		var num int64
		checkErr(rows.Scan(&num))
		//resp = append(resp, wordU{IdParagraph:paragraph, IdSentence:sentence, Word:word, IdTag:tag})
	}
	json.NewEncoder(w).Encode(resp)
}

type Consumer struct {
	name string
}

func NewConsumer(tag int) *Consumer {
	return &Consumer{
		name: fmt.Sprintf("consumer%d", tag),
	}
}
func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	parsedData := delivery.Payload()
	delivery.Ack()
	tmpText := word{}
	err := json.Unmarshal([]byte(parsedData), &tmpText)
	checkErr(err)
	tx, err := db.Begin()
	checkErr(err)
	qi1, err := tx.Prepare("INSERT INTO text (id_document, id_paragraph, id_sentence, id_word, word, tag) VALUES (?, ?, ?, ?, ?, ?)")
	checkErr(err)
	if _, err := qi1.Exec(tmpText.UUId, tmpText.IdParagraph, tmpText.IdSentence, tmpText.IdWord, tmpText.Word, tmpText.IdTag); err != nil {
		log.Fatal(err)
	}
	checkErr(tx.Commit())
}

func listenQueueRedis() {
	connection := rmq.OpenConnection("consumer", "tcp", "redis:6379", 1)
	doc := connection.OpenQueue("processed_data")
	doc.StartConsuming(1000, 500*time.Millisecond)
	doc.AddConsumer("consumer", NewConsumer(0))
	select {}
}

func main() {
	var err error
	db, err = sql.Open("clickhouse", CONNECTION)

	checkErr(err)
	if err := db.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s", exception.Code, exception.Message)
		} else {
			fmt.Println(err)
		}
	}
	_, err = db.Exec(`
			CREATE TABLE IF NOT EXISTS document (
				id UUID,
				name        String,
				time   DateTime
			) engine=Memory
		`)
	checkErr(err)
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS text (
			id_document UUID,
			id_paragraph  Int64,
			id_sentence  Int64,
			id_word  Int64,
			word  String,
			tag  Int64
		) engine=Memory
	`)
	checkErr(err)
	router := mux.NewRouter().StrictSlash(true)
	go listenQueueRedis()
	router.HandleFunc("/", homePage)
	router.HandleFunc("/documents", uploadDocument).Methods("POST")
	router.HandleFunc("/documents", getDocuments).Methods("GET")
	router.HandleFunc("/documents/{id}/text", getDocText).Methods("GET")
	router.HandleFunc("/documents/{token}", getStatistic).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))
}
