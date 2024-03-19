package rimbus

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type RabbitMqClient struct {
	cfg     *RabbitMQConfig
	engine  *amqp.Connection
	channel *amqp.Channel
}

func (client *RabbitMqClient) Consume(topic string) (<-chan *MessageEvent, error) {
	log.Printf("<[🔥]> Consuming messages from topic ::::: | %s <[🔥]> \n", topic)

	// Setup RabbitMQ channel and other necessary configurations
	queue, err := client.channel.QueueDeclare(
		topic, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Println("‼️failed to declare queue topic ::::: |", err)
		return nil, err
	}

	messageEvents := make(chan *MessageEvent)

	// Set up a channel for handling Ctrl-C, etc
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming messages
	go func() {
		// Implement RabbitMQ message consumption logic here
		for {
			select {
			case sig := <-sigChan:
				log.Printf("‼️Caught signal %v: terminating\n", sig)
				// Close connections and cleanup
				return
			default:
				// Consume messages from RabbitMQ
				// Example code to consume messages
				// Replace this with your RabbitMQ message consumption logic
				// Consume message from RabbitMQ queue
				messages, err := client.channel.Consume(
					queue.Name, // queue name
					"",         // consumer name
					true,       // auto-acknowledge messages
					false,      // exclusive
					false,      // no-local
					false,      // no-wait
					nil,        // args
				)
				if err != nil {
					log.Printf("‼️Failed to consume messages from RabbitMQ: %v\n", err)
					continue
				}

				for msg := range messages {
					// Parse message body
					evt := new(MessageEvent)
					if err := json.Unmarshal(msg.Body, evt); err != nil {
						log.Printf("‼️Failed to unmarshal message body: %v\n", err)
						continue
					}

					messageEvents <- evt
					log.Printf("[👽] Message processed to the channel :::::: | %s [👽]\n", msg.RoutingKey)
				}
			}
		}
	}()

	return messageEvents, nil
}

func (client *RabbitMqClient) Publish(topic string, message *MessageEvent) (chan kafka.Event, error) {
	// We create a Queue to send the message to.
	queue, err := client.channel.QueueDeclare(
		topic, // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	jb, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	if err := client.channel.PublishWithContext(context.Background(), client.cfg.Exchange,
		queue.Name, false, false, amqp.Publishing{ContentType: "application/json", Body: jb}); err != nil {
		return nil, err
	}

	log.Printf("--- Sending to Queue: %s ---\n --- [x] Send %s--- \n", queue.Name, message)
	return nil, nil
}

func (client *RabbitMqClient) GetEngine() interface{} {
	return client.engine
}

func (client *RabbitMqClient) EstablishConnection() {
	var counts int64
	var backOff = 1 * time.Second

	dsn := client.GetDSN()

	for {
		c, err := amqp.Dial(dsn)
		if err != nil {
			log.Println("RabbitMQ not yet ready to connect!", err)
			counts++
		} else {
			log.Println("Connected to client successfully!")
			client.engine = c
			break
		}

		if counts > 5 {
			log.Println("failed to connect to rabbitmq:", err)
			os.Exit(1)
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Println("Backing off...")
		time.Sleep(backOff)
	}

	if err := client.createChannel(); err != nil {
		log.Fatalln("failed to create channel:", err)
	}
}

func (client *RabbitMqClient) createChannel() error {
	var err error
	client.channel, err = client.engine.Channel()
	if err != nil {
		log.Println("‼️failed to create channel ::::: |", err)
		return err
	}

	return nil
}

func (client *RabbitMqClient) GetDSN() string {
	dsn := fmt.Sprintf(
		"%s://%s:%s@%s:%s/",
		client.cfg.Protocol,
		client.cfg.Auth.User,
		client.cfg.Auth.Password,
		client.cfg.Host,
		client.cfg.Port,
	)
	log.Printf("RabbitMQ DSN: %s\n", dsn)

	return dsn
}

func NewRabbitMQClient(params *RabbitMQConfig) MessageBusClient {
	return &RabbitMqClient{cfg: params}
}
