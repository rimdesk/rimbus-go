package rimbus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMqClient struct {
	cfg     *RabbitMQConfig
	engine  *amqp.Connection
	channel *amqp.Channel
	ctx     context.Context
	wg      sync.WaitGroup
	mu      sync.Mutex // Protects channel and connection during reconnection

}

func (client *RabbitMqClient) reconnect() {
	client.mu.Lock()
	defer client.mu.Unlock()

	var err error
	var attempts int64 = 0
	backOff := 1 * time.Second

	for {
		attempts++
		client.engine, err = amqp.Dial(client.GetDSN())
		if err == nil {
			client.channel, err = client.engine.Channel()
			if err == nil {
				log.Println("✅ Reconnected to RabbitMQ successfully!")
				return
			}
			log.Printf("‼️ Failed to create channel: %v\n", err)
			client.engine.Close() // Close the connection if channel creation fails
		}

		if attempts > 5 {
			log.Printf("‼️ Failed to reconnect after %d attempts: %v\n", attempts, err)
			os.Exit(1)
		}

		backOff = time.Duration(attempts) * time.Second
		log.Printf("Reconnection attempt %d failed, backing off for %v...\n", attempts, backOff)
		time.Sleep(backOff)
	}
}

func (client *RabbitMqClient) Consume(topicName string, opt *ConsumeOptions) (<-chan *MessageEvent, error) {
	log.Printf("<[🔥]> Consuming messages from topicName: %s <[🔥]>\n", topicName)

	// Declare queue
	queue, err := client.channel.QueueDeclare(
		topicName,      // name
		opt.Durable,    // durable
		opt.AutoDelete, // delete when unused
		opt.Exclusive,  // exclusive
		opt.NoWait,     // no-wait
		opt.Args,       // arguments
	)
	if err != nil {
		log.Printf("‼️ Failed to declare queue for topic %s: %v\n", topicName, err)
		return nil, err
	}

	// Optional: Bind queue to exchange if needed
	if opt.Exchange != "" {
		if err := client.channel.QueueBind(
			queue.Name,   // queue name
			topicName,    // routing key
			opt.Exchange, // exchange name
			opt.NoWait,
			opt.Args,
		); err != nil {
			log.Printf("‼️ Failed to bind queue to exchange for topic %s: %v\n", topicName, err)
			return nil, err
		}
	}

	messageEvents := make(chan *MessageEvent)

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	client.wg.Add(1) // Add to wait group for the consumer goroutine

	go func() {
		defer client.wg.Done()     // Decrement wait group counter when the goroutine exits
		defer close(messageEvents) // Close channel when done

		for {
			select {
			case sig := <-sigChan:
				log.Printf("‼️ Caught signal %v: initiating shutdown\n", sig)
				client.Close() // Close the connection and channel
				return
			default:
				// Check if the connection or channel is closed, and attempt reconnection if necessary
				client.mu.Lock()
				if client.channel == nil || client.channel.IsClosed() || client.engine.IsClosed() {
					log.Println("‼️ Connection or channel closed, attempting to reconnect...")
					client.reconnect() // Attempt to reconnect
				}
				client.mu.Unlock()

				// Consume messages
				messages, err := client.channel.Consume(
					queue.Name,
					opt.ConsumerName,
					opt.AutoAck,
					opt.Exclusive,
					opt.NoLocal,
					opt.NoWait,
					opt.Args,
				)
				if err != nil {
					log.Printf("‼️ Failed to consume messages: %v\n", err)
					time.Sleep(1 * time.Second) // Retry delay
					continue
				}

				for msg := range messages {
					evt := new(MessageEvent)
					if err := json.Unmarshal(msg.Body, evt); err != nil {
						log.Printf("‼️ Failed to unmarshal message: %v\n", err)
						continue
					}

					evt.Acknowledger = msg.Acknowledger
					evt.Tag = msg.DeliveryTag
					messageEvents <- evt
					log.Printf("<[✈️]> Message sent to topicName %s <[✈️]>\n", msg.RoutingKey)
				}
			}
		}
	}()

	return messageEvents, nil
}

func (client *RabbitMqClient) Publish(topic string, message []byte, opt *PublishOptions) (bool, error) {
	queue, err := client.channel.QueueDeclare(
		topic,
		opt.Durable,
		opt.AutoDelete,
		opt.Exclusive,
		opt.NoWait,
		opt.Args,
	)
	if err != nil {
		return false, err
	}

	// Optional: Bind the queue to the specified exchange if provided
	if opt.Exchange != "" {
		if err := client.channel.QueueBind(
			queue.Name,   // queue name
			topic,        // routing key (use topic as routing key)
			opt.Exchange, // exchange name
			opt.NoWait,   // no-wait
			opt.Args,     // arguments
		); err != nil {
			log.Printf("‼️ Failed to bind queue to exchange %s for topic %s: %v\n", opt.Exchange, topic, err)
			return false, err
		}
	}

	if err := client.channel.PublishWithContext(client.ctx, opt.Exchange, queue.Name, opt.Mandatory, opt.Immediately, amqp.Publishing{
		ContentType: opt.ContentType,
		Body:        message,
	}); err != nil {
		return false, err
	}

	log.Printf("--- Sent to Queue: %s --- [x] Sent %s ---\n", queue.Name, message)
	return true, nil
}

func (client *RabbitMqClient) GetEngine() interface{} {
	return client.engine
}

func (client *RabbitMqClient) EstablishConnection() {
	var counts int64
	backOff := 1 * time.Second

	dsn := client.GetDSN()

	for {
		c, err := amqp.Dial(dsn)
		if err != nil {
			log.Println("‼️RabbitMQ connection failed:", err)
			counts++
		} else {
			log.Println("Connected to RabbitMQ!")
			client.engine = c
			break
		}

		if counts > 5 {
			log.Println("‼️Failed to connect to RabbitMQ after multiple attempts:", err)
			os.Exit(1)
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Println("‼️Backing off...")
		time.Sleep(backOff)
	}

	if err := client.createChannel(); err != nil {
		log.Fatalln("Failed to create channel:", err)
	}
}

func (client *RabbitMqClient) createChannel() error {
	var err error
	client.channel, err = client.engine.Channel()
	if err != nil {
		log.Println("‼️ Failed to create channel:", err)
		return err
	}
	return nil
}

func (client *RabbitMqClient) GetDSN() string {
	dsn := fmt.Sprintf("%s://%s:%s@%s:%s/",
		client.cfg.Protocol,
		client.cfg.Auth.User,
		client.cfg.Auth.Password,
		client.cfg.Host,
		client.cfg.Port,
	)
	log.Printf("RabbitMQ DSN: %s\n", dsn)
	return dsn
}

func (client *RabbitMqClient) Close() {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.channel != nil && !client.channel.IsClosed() {
		if err := client.channel.Close(); err != nil {
			log.Printf("Failed to close RabbitMQ channel: %v\n", err)
		}
	}
	if client.engine != nil && !client.engine.IsClosed() {
		if err := client.engine.Close(); err != nil {
			log.Printf("Failed to close RabbitMQ connection: %v\n", err)
		}
	}
	client.wg.Wait() // Wait for all goroutines to finish
}

func NewRabbitMQClient(ctx context.Context, params *RabbitMQConfig) *RabbitMqClient {
	client := &RabbitMqClient{cfg: params, ctx: ctx}
	return client
}
