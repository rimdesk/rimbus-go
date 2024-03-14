package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rimdesk/eventbus-go/eventbus"
	"log"
)

var ()

func main() {
	messageClient := eventbus.NewClient(
		&eventbus.ConfigParams{
			Engine: "kafka", // rabbitmq, kafka
			Params: &eventbus.BrokerParams{
				KafkaConfig: &kafka.ConfigMap{
					"bootstrap.servers": "0.0.0.0:19092",
					//"group.id":          "", // product-api
					//"auto.offset.reset": "earliest",
				},
			},
		},
	)

	event := eventbus.NewEvent("product-api", "inventory.create", "product.create")
	event.Payload = map[string]any{
		"company": map[string]any{
			"id":                  "bb4ef24b-1699-4452-ad09-f284e57c6049",
			"name":                "Acme Corporation",
			"email":               "contact@acme.com",
			"phone_number":        "+1234567890",
			"tin_number":          "1234567890",
			"registration_number": "ACME123",
			"currency":            "USD",
			"category_id":         "123",
		},
	}

	if err := messageClient.Publish("rimdesk.products", event); err != nil {
		log.Fatalln("failed to send message: |", err)
	}

	messageEvents, err := messageClient.Consume("inventory.create")
	if err != nil {
		log.Println("failed to consume messages :::::: |", err)
	}

	for message := range messageEvents {
		fmt.Println("Message received :::::: |", message)
	}
}