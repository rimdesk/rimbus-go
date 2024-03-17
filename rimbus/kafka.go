package rimbus

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type KClient struct {
	cfg *Params
}

func (client *KClient) Consume(topic string) (<-chan *MessageEvent, error) {
	log.Printf("<[🔥]> Consuming messages from topic ::::: | %s <[🔥]> \n", topic)
	consumer, err := kafka.NewConsumer(client.getConfig())
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

func (client *KClient) Connect() {
}

func (client *KClient) GetDSN() string {
	//TODO implement me
	panic("implement me")
}

func (client *KClient) GetEngine() interface{} {
	return nil
}

func (client *KClient) Publish(topic string, message *MessageEvent) (chan kafka.Event, error) {
	producer, err := kafka.NewProducer(client.getConfig())
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

func (client *KClient) readConfigFile(configFile string) kafka.ConfigMap {
	m := make(map[string]kafka.ConfigValue)

	file, err := os.Open(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to open file: %s", err)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "#") && len(line) != 0 {
			before, after, found := strings.Cut(line, "=")
			if found {
				parameter := strings.TrimSpace(before)
				value := strings.TrimSpace(after)
				m[parameter] = value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Failed to read file: %s", err)
		os.Exit(1)
	}

	return m
}

func (client *KClient) readFromMap(data map[string]any) kafka.ConfigMap {
	cfgMap := make(map[string]kafka.ConfigValue)
	for s, a := range data {
		cfgMap[s] = a
	}

	return cfgMap
}

func (client *KClient) getConfig() *kafka.ConfigMap {
	var cfgMap kafka.ConfigMap
	if client.cfg.File != "" {
		cfgMap = client.readConfigFile(client.cfg.File)
		return &cfgMap
	}

	cfgMap = client.readFromMap(client.cfg.Map)

	return &cfgMap
}

func NewKafkaClient(p *Params) MessageBusClient {
	return &KClient{p}
}
