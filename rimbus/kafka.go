package rimbus

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type KClient struct {
	cfg *BrokerParams
}

func (client *KClient) Consume(topic string) (<-chan *MessageEvent, error) {
	consumer, err := kafka.NewConsumer(client.cfg.KafkaConfig)
	if err != nil {
		log.Println("‼️failed to register consumer ::::: |", err)
		return nil, err
	}

	if err := consumer.Subscribe(topic, nil); err != nil {
		log.Printf("‼️failed to subscribe to topc: %s ::::: |%v\n", topic, err)
		return nil, err
	}

	// Set up a channel for handling Ctrl-C, etc
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	messageEvents := make(chan *MessageEvent)

	// Process messages
	run := true
	for run {
		select {
		case sig := <-sigChan:
			log.Printf("‼️Caught signal %v: terminating\n", sig)
			run = false
		default:
			message, err := consumer.ReadMessage(1000 * time.Second)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				log.Println("‼️failed to read consumer message :::: |", err)
				continue
			}

			evt := new(MessageEvent)
			if err := json.Unmarshal(message.Value, evt); err != nil {
				log.Println("‼️failed to unmarshal message value :::: |", err)
				continue
			}

			messageEvents <- evt
			fmt.Printf("[👽] Message processed to the channel :::::: | %s [👽]\n", message.TopicPartition.String())
		}
	}

	defer consumer.Close()

	return messageEvents, nil
}

func (client *KClient) Connect() {
}

func (client *KClient) GetDSN() string {
	//TODO implement me
	panic("implement me")
}

func (client *KClient) GetEngine() interface{} {
	return nil
}

func (client *KClient) Publish(topic string, message *MessageEvent) error {
	producer, err := kafka.NewProducer(client.cfg.KafkaConfig)
	if err != nil {
		log.Println("‼️failed to create producer ::::: |", err)
		return err
	}

	log.Printf("<[🔥]> Sending message to topic: %s <[🔥]>\n", topic)
	jb, err := json.Marshal(message)
	if err != nil {
		log.Println("‼️failed to marshal data ::::: |", err)
		return err
	}

	deliveryChan := make(chan kafka.Event, 10000)
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          jb,
		Timestamp:      time.Now(),
	}, deliveryChan); err != nil {
		log.Println("‼️failed to send message into topic ::::: |", err)
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("‼️delivery topic partition failed: %v\n", m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	log.Printf("<[✈️]> Message sent to topic %s [%d] at offset %v <[✈️]>\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	close(deliveryChan)
	return nil
}

func NewKafkaClient(p *BrokerParams) MessageBusClient {
	return &KClient{cfg: p}
}
