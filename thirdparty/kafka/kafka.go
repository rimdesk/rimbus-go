package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rimdesk/eventbus-go"
	"log"
	"os"
)

type KClient struct {
	producer *kafka.Producer
}

func (client *KClient) Subscribe(topic string) (<-chan eventbus.MessageEvent, error) {
	//TODO implement me
	panic("implement me")
}

func (client *KClient) Connect() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": client.GetDSN(),
		"client.id":         os.Getenv("APP.NAME"),
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	client.producer = p
}

func (client *KClient) GetDSN() string {
	return fmt.Sprintf("%s:%s",
		os.Getenv("BROKER.HOST"),
		os.Getenv("BROKER.PORT"),
	)
}

func (client *KClient) GetEngine() interface{} {
	return client.producer
}

func (client *KClient) Publish(topic string, message interface{}) error {
	jb, err := json.Marshal(message)
	if err != nil {
		return err
	}

	deliveryChan := make(chan kafka.Event, 10000)
	if err := client.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          jb,
	}, deliveryChan); err != nil {
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return m.TopicPartition.Error
	}

	log.Printf("Delivered message to topic %s [%d] at offset %v\n",
		*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	close(deliveryChan)
	return nil
}
