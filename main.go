package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"github.com/nats-io/stan.go"
	_"github.com/lib/pq"
)

type Data struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

var (
	cache = make(map[string]Data)
	mu    sync.RWMutex
	db    *sql.DB
)

func main() {
	connectPostgres()
	restoreCache()

	sc, err := stan.Connect("test-cluster", "subscriber", stan.NatsURL("nats://localhost:4222"))
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()

	_, err = sc.Subscribe("data", func(msg *stan.Msg) {
		var data Data
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			log.Println("Invalid message format")
			return
		}
		saveToPostgres(data)
		saveToCache(data)
	}, stan.DeliverAllAvailable())
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/data", getDataHandler)
	http.ListenAndServe(":8080", nil)
}

func connectPostgres() {
	var err error
	db, err = sql.Open("postgres", "host=localhost user=postgres password=postgres dbname=test sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS data (id TEXT PRIMARY KEY, name TEXT, value TEXT)`); err != nil {
		log.Fatal(err)
	}
}

func saveToPostgres(data Data) {
	_, err := db.Exec(`INSERT INTO data (id, name, value) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING`, data.ID, data.Name, data.Value)
	if err != nil {
		log.Println("Failed to save to Postgres:", err)
	}
}

func saveToCache(data Data) {
	mu.Lock()
	defer mu.Unlock()
	cache[data.ID] = data
}

func restoreCache() {
	rows, err := db.Query(`SELECT id, name, value FROM data`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var data Data
		if err := rows.Scan(&data.ID, &data.Name, &data.Value); err != nil {
			continue
		}
		cache[data.ID] = data
	}
}

func getDataHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	mu.RLock()
	data, exists := cache[id]
	mu.RUnlock()

	if !exists {
		http.Error(w, "Data not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(data)
}
