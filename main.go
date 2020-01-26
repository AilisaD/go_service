package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/adjust/rmq"
	"github.com/go-redis/redis"
	_ "github.com/go-redis/redis/v7"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var CONNECTION = "tcp://clickhouse:8123?username=default&database=default&compress=true&&&alt_hosts=clickhouse:9000,clickhouse:9009"

var db *sql.DB
var client *redis.Client

type document struct {
	UUId       uuid.UUID `json:"document_id"`
	Name       string    `json:"name"`
	TimeUpload time.Time `json:"uploaded_at"`
}

type word struct {
	UUId        string `json:"document_uuid"`
	IdParagraph int64  `json:"paragraph"`
	IdSentence  int64  `json:"sentence"`
	IdWord      int64  `json:"object"`
	Word        string `json:"token"`
	IdTag       int64  `json:"tag"`
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
type Stat []int64

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func homePage(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	err := json.NewEncoder(w).Encode(document{UUId: uuid.New(), Name: "Hello world", TimeUpload: time.Now()})
	checkErr(err)
}

func uploadDocument(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(1000000)
	checkErr(err)
	file, handler, err := r.FormFile("file")
	checkErr(err)
	if strings.Contains(handler.Filename, ".docx") {
		if db == nil {
			err := json.NewEncoder(w).Encode("DB connection lost")
			checkErr(err)
			return
		}
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
		defer file.Close()
		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, file); err != nil {
			log.Fatal(err)
		}
		err = client.Set(fmt.Sprintf("file:%s", uidd.String()), buf.Bytes(), 0).Err()
		checkErr(err)
		redisQueue := rmq.OpenConnection("producer", "tcp", "redis:6379", 0)
		doc := redisQueue.OpenQueue("documents")
		doc.Publish(uidd.String())
		w.WriteHeader(201)
	}
}

func getDocuments(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
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
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	params := mux.Vars(r)
	limit, _ := strconv.Atoi(r.FormValue("limit"))
	offset, _ := strconv.Atoi(r.FormValue("offset"))
	log.Println(limit, offset)
	if db == nil {
		json.NewEncoder(w).Encode("DB connection lost")
		return
	}
	rows, err := db.Query(
		"SELECT id_paragraph, id_sentence, word, tag FROM text WHERE id_document=toUUID(?) ORDER BY id_paragraph, id_sentence, id_word limit ?, ?", params["id"], offset, limit)

	if err != nil {
		w.WriteHeader(400)
		log.Println(err)
		return
	}
	resp := TextU{}
	for rows.Next() {
		var (
			paragraph int64
			sentence  int64
			word      string
			tag       int64
		)
		rows.Scan(&paragraph, &sentence, &word, &tag)
		resp = append(resp, wordU{IdParagraph: paragraph, IdSentence: sentence, Word: word, IdTag: tag})
	}
	json.NewEncoder(w).Encode(resp)
}

func getDB(w http.ResponseWriter, r *http.Request) {
	//w.Header().Set("Content-Type", "application/json; charset=utf-8")
	//rows, err := db.Query("SELECT word from text")
}

func getStatistic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	params := mux.Vars(r)
	rows, err := db.Query("SELECT countIf(tag=0) as tag0,  countIf(tag=1) as tag1, countIf(tag=2) as tag2, countIf(tag=3) as tag3, "+
		"countIf(tag=4) as tag4, countIf(tag=5) as tag5, "+
		"countIf(tag=6) as tag6,  countIf(tag=7) as tag7 "+
		"from text WHERE lowerUTF8(word)=lowerUTF8(?) and id_document=toUUID(?)", params["token"], params["id"])
	if err != nil {
		w.WriteHeader(400)
		return
	}
	resp := []int64{}
	for rows.Next() {
		log.Printf("Next for tags")
		var (
			t0 int64
			t1 int64
			t2 int64
			t3 int64
			t4 int64
			t5 int64
			t6 int64
			t7 int64
		)
		rows.Scan(&t0, &t1, &t2, &t3, &t4, &t5, &t6, &t7)
		resp = []int64{t0, t1, t2, t3, t4, t5, t6, t7}
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
	log.Println(parsedData)
	err := json.Unmarshal([]byte(parsedData), &tmpText)
	checkErr(err)
	tx, err := db.Begin()
	checkErr(err)
	qi1, err := tx.Prepare("INSERT INTO text (id_document, id_paragraph, id_sentence, id_word, word, tag) VALUES (?, ?, ?, ?, ?, ?)")
	checkErr(err)

	if _, err := qi1.Exec(uuid.MustParse(tmpText.UUId), tmpText.IdParagraph, tmpText.IdSentence, tmpText.IdWord, tmpText.Word, tmpText.IdTag); err != nil {
		log.Fatal(err)
	}
	checkErr(tx.Commit())
}

func listenQueueRedis() {
	connection := rmq.OpenConnection("consumer", "tcp", "redis:6379", 0)
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
	client = redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	router := mux.NewRouter().StrictSlash(true)
	go listenQueueRedis()
	router.HandleFunc("/", homePage)
	router.HandleFunc("/documents", uploadDocument).Methods("POST")
	router.HandleFunc("/documents", getDocuments).Methods("GET")
	router.HandleFunc("/documents/{id}/text", getDocText).Methods("GET")
	router.HandleFunc("/documents/{id}/token_info/{token}", getStatistic).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", router))
}
