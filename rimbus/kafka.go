package rimbus

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type KafkaClient struct {
	cfg *KafkaConfig
}

func (client *KafkaClient) Consume(topic string) (<-chan *MessageEvent, error) {
	log.Printf("<[🔥]> Consuming messages from topic ::::: | %s <[🔥]> \n", topic)
	configMap := client.getConfig("consumer")
	consumer, err := kafka.NewConsumer(&configMap)
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
			log.Printf("[👽] Message processed to the channel :::::: | %s [👽]\n", message.TopicPartition.String())
		}
	}

	defer consumer.Close()

	return messageEvents, nil
}

func (client *KafkaClient) Connect() {
}

func (client *KafkaClient) GetDSN() string {
	//TODO implement me
	panic("implement me")
}

func (client *KafkaClient) GetEngine() interface{} {
	return nil
}

func (client *KafkaClient) Publish(topic string, message *MessageEvent) (chan kafka.Event, error) {
	configMap := client.getConfig("producer")
	producer, err := kafka.NewProducer(&configMap)
	if err != nil {
		log.Println("‼️failed to create producer ::::: |", err)
		return nil, err
	}

	log.Printf("<[🔥]> Sending message to topic: %s <[🔥]>\n", topic)
	jb, err := json.Marshal(message)
	if err != nil {
		log.Println("‼️failed to marshal data ::::: |", err)
		return nil, err
	}

	deliveryChan := make(chan kafka.Event, 10000)
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          jb,
		Timestamp:      time.Now(),
	}, deliveryChan); err != nil {
		log.Println("‼️failed to send message into topic ::::: |", err)
		return nil, err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("‼️delivery topic partition failed: %v\n", m.TopicPartition.Error)
		return nil, m.TopicPartition.Error
	}

	log.Printf("<[✈️]> Message sent to topic %s [%d] at offset %v <[✈️]>\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	close(deliveryChan)
	return deliveryChan, nil
}

func (client *KafkaClient) getConfig(key string) kafka.ConfigMap {
	cfgMap := make(map[string]kafka.ConfigValue)
	cfgMap["bootstrap.servers"] = client.cfg.Servers

	log.Printf("Kafka Servers: %s\n", client.cfg.Servers)

	if key == "consumer" {
		log.Printf("Kafka Consumer Group: %s\n", client.cfg.Consumer.Group)
		log.Printf("Kafka Consumer Start: %s\n", client.cfg.Consumer.Start)

		cfgMap["group.id"] = client.cfg.Consumer.Group
		cfgMap["auto.offset.reset"] = client.cfg.Consumer.Start
	}

	if key == "producer" {
		// Load producer configuration: client.cfg.Producer
	}

	return cfgMap
}

func NewKafkaClient(p *KafkaConfig) MessageBusClient {
	return &KafkaClient{p}
}
