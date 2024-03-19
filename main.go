package main

import (
	"fmt"
	"github.com/rimdesk/rimbus-go/rimbus"
	"log"
)

func main() {
	client := rimbus.New()
	event := rimbus.NewEvent(rimbus.ProductApi, rimbus.InventoryCreateEvent, rimbus.ProductCreateEvent)
	event.Payload = map[string]any{
		"product": map[string]any{
			"id":           "bb4ef24b-1699-4452-ad09-f284e57c6049",
			"barcode":      "00022233444",
			"name":         "Apple iPhone Charger",
			"description":  "Apple iPhone Charger Description",
			"supply_price": 800,
			"retail_price": 1000,
			"type":         "product",
			"amount":       1200,
			"category_id":  "123",
			"company_id":   "3ec34288-5ce8-4974-b05e-6e50a32465bb",
		},
	}
	event.Metadata = map[string]any{
		"triggered_by": "bb4ef24b-1699-4452-ad09-f284e57c6049",
	}

	evt, err := client.Publish(rimbus.AppProductTopic, event)
	if err != nil {
		log.Fatalln("failed to send message: |", err)
	}

	log.Println("Published event ::::: |", evt)

	messageEvents, err := client.Consume(rimbus.AppProductTopic)
	if err != nil {
		log.Println("failed to consume messages :::::: |", err)
	}

	for message := range messageEvents {
		fmt.Println("Message received :::::: |", message)
	}
}
