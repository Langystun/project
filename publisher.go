package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/stan.go"
)

type PublishData struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

func Tmain() {
	sc, err := stan.Connect("test-cluster", "publisher", stan.NatsURL("nats://localhost:4222"))
	if err != nil {
		log.Fatal(err)
	}
	defer sc.Close()

	data := Data{
		ID:    "1",
		Name:  "Test",
		Value: "123",
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)
	}

	for {
		if err := sc.Publish("data", jsonData); err != nil {
			log.Println("Failed to publish:", err)
		} else {
			log.Println("Message published")
		}
		time.Sleep(5 * time.Second)
	}
}
