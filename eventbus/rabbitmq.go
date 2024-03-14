package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math"
	"os"
	"time"
)

type MqClient struct {
	cfg     *BrokerParams
	engine  *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func (rabbit *MqClient) Consume(topic string) (<-chan *MessageEvent, error) {
	//TODO implement me
	panic("implement me")
}

func (rabbit *MqClient) Publish(topic string, message *MessageEvent) error {
	bodyData, err := json.Marshal(message)
	if err != nil {
		return err
	}

	if err := rabbit.channel.PublishWithContext(context.Background(), os.Getenv("BROKER.EXCHANGE"),
		rabbit.queue.Name, false, false, amqp.Publishing{ContentType: "application/json", Body: bodyData}); err != nil {
		return err
	}

	log.Printf("--- Sending to Queue: %s ---\n --- [x] Send %s--- \n", rabbit.queue.Name, message)
	return nil
}

func (rabbit *MqClient) GetEngine() interface{} {
	return rabbit.engine
}

func (rabbit *MqClient) Connect() {
	var counts int64
	var backOff = 1 * time.Second

	dsn := rabbit.GetDSN()

	for {
		c, err := amqp.Dial(dsn)
		if err != nil {
			log.Println("RabbitMQ not yet ready to connect!")
			counts++
		} else {
			log.Println("Connected to rabbit successfully!")
			rabbit.engine = c
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

	if err := rabbit.createChannel(); err != nil {
		log.Fatalln("failed to create channel:", err)
	}

	if err := rabbit.createQueue(); err != nil {
		log.Fatalln("failed to create queue:", err)
	}
}

func (rabbit *MqClient) createChannel() error {
	var err error
	rabbit.channel, err = rabbit.engine.Channel()
	if err != nil {
		return err
	}

	return nil
}

func (rabbit *MqClient) createQueue() error {
	var err error
	rabbit.queue, err = rabbit.channel.QueueDeclare(
		os.Getenv("BROKER.QUEUE"),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	return nil
}

func (rabbit *MqClient) GetDSN() string {
	log.Println("Building RabbitMQ DSN in parts")
	dsn := fmt.Sprintf("%s://%s:%s@%s:%s",
		os.Getenv("BROKER.PROTOCOL"),
		os.Getenv("BROKER.USER"),
		os.Getenv("BROKER.PASSWORD"),
		os.Getenv("BROKER.HOST"),
		os.Getenv("BROKER.PORT"),
	)

	log.Printf("RabbitMQ DSN: %s\n", dsn)

	return dsn
}

func NewRabbitMQClient(params *BrokerParams) MessageBusClient {
	return &MqClient{cfg: params}
}
