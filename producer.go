package main

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rimdesk/rimbus-go/rimbus"
	"log"
)

func main() {
	// Here we connect to RabbitMQ or send a message if there are any errors connecting.
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("%s: %s", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("%s:", err)
	}
	defer ch.Close()

	// We create a Queue to send the message to.
	q, err := ch.QueueDeclare(
		"golang-queue", // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		log.Fatalf("%s:", err)
	}

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

	jb, err := json.Marshal(event)
	if err != nil {
		log.Fatalf("%s:", err)
	}

	// We set the payload for the message.
	err = ch.PublishWithContext(
		context.TODO(),
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(jb),
		})

	// If there is an error publishing the message, a log will be displayed in the terminal.
	if err != nil {
		log.Fatalf("%s:", err)
	}
	log.Printf(" [x] Congrats, sending message: %s", string(jb))
}
