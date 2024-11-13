package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rimdesk/rimbus-go/rimbus"
	"log"
)

func main() {
	client := rimbus.New(context.TODO(), rimbus.Dev)
	client.EstablishConnection() // Only necessary for RabbitMQ
	event := rimbus.NewEvent("rimdesk.product-api", "rimdesk.inventory.create", "rimdesk.product.create")
	event.Metadata = map[string]any{
		"triggered_by": "bb4ef24b-1699-4452-ad09-f284e57c6049",
	}

	payload := map[string]any{
		"product": map[string]any{
			"id":           "bb4ef24b-1699-4452-ad09-f284e57c6049",
			"barcode":      "1234567890",
			"name":         "Apple iPhone Charger",
			"description":  "Apple iPhone Charger Description",
			"supply_price": 800.40,
			"retail_price": 1000.85,
			"type":         "product",
			"amount":       1200.0,
			"category_id":  "123",
			"company_id":   "3ec34288-5ce8-4974-b05e-6e50a32465bb",
		},
	}

	jb, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("failed to marshal payload", err)
	}

	event.Payload = jb

	jb, err = json.Marshal(event)
	if err != nil {
		log.Fatalln("failed to marshal event:", err)
	}

	if _, err := client.Publish("rimdesk.product-api", jb, &rimbus.PublishOptions{
		Args:        nil,
		AutoDelete:  false,
		ContentType: "application/json",
		Durable:     false,
		Exclusive:   false,
		Exchange:    "",
		Mandatory:   false,
		NoWait:      false,
		Immediately: false,
	}); err != nil {
		log.Fatalln("failed to send message: |", err)
	}

	messageEvents, err := client.Consume("rimdesk.product-api", &rimbus.ConsumeOptions{
		AutoAck:      false,
		ConsumerName: "",
		QueueName:    "rimdesk.product-api",
		Durable:      false,
		Exclusive:    false,
		NoLocal:      false,
		NoWait:       false,
	})
	if err != nil {
		log.Println("failed to consume messages :::::: |", err)
	}

	for message := range messageEvents {
		if err := message.Acknowledger.Ack(message.Tag, false); err != nil {
			log.Fatalln(fmt.Errorf("failed to acknowledge message: %s", err))
		}
	}
}
