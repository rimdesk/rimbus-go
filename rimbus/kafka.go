package rimbus

import (
	"context"
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

func (client *KafkaClient) Consume(topic string, opt *ConsumeOptions) (<-chan *MessageEvent, error) {
	log.Printf("<[🔥]> Consuming messages from topic ::::: | %s <[🔥]> \n", topic)

	configMap := client.getConfig("consumer")
	consumer, err := kafka.NewConsumer(&configMap)
	if err != nil {
		log.Println("‼️failed to register consumer ::::: |", err)
		return nil, err
	}
	defer consumer.Close()

	if err := consumer.Subscribe(topic, nil); err != nil {
		log.Printf("‼️failed to subscribe to topic: %s ::::: |%v\n", topic, err)
		return nil, err
	}

	// Set up a channel for message events
	messageEvents := make(chan *MessageEvent)

	// Set up a channel for handling signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consuming messages
	go func() {
		defer close(messageEvents)

		for {
			select {
			case sig := <-sigChan:
				log.Printf("‼️Caught signal %v: terminating\n", sig)
				return
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
	}()

	return messageEvents, nil
}

func (client *KafkaClient) EstablishConnection() {
	log.Println("<[🔥]> Established kafka connection successful! <[🔥]>")
}

func (client *KafkaClient) GetDSN() string {
	//TODO implement me
	panic("implement me")
}

func (client *KafkaClient) GetEngine() interface{} {
	return nil
}

func (client *KafkaClient) Publish(topic string, message []byte, opt *PublishOptions) (bool, error) {
	configMap := client.getConfig("producer")
	producer, err := kafka.NewProducer(&configMap)
	if err != nil {
		log.Println("‼️failed to create producer ::::: |", err)
		return false, err
	}

	log.Printf("<[🔥]> Sending message to topic: %s <[🔥]>\n", topic)
	jb, err := json.Marshal(message)
	if err != nil {
		log.Println("‼️failed to marshal data ::::: |", err)
		return false, err
	}

	deliveryChan := make(chan kafka.Event, 10000)
	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          jb,
		Timestamp:      time.Now(),
	}, deliveryChan); err != nil {
		log.Println("‼️failed to send message into topic ::::: |", err)
		return false, err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("‼️delivery topic partition failed: %v\n", m.TopicPartition.Error)
		return false, m.TopicPartition.Error
	}

	log.Printf("<[✈️]> Message sent to topic %s [%d] at offset %v <[✈️]>\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)

	close(deliveryChan)

	return true, nil
}

func (client *KafkaClient) getConfig(key string) kafka.ConfigMap {
	cfgMap := make(map[string]kafka.ConfigValue)
	cfgMap["bootstrap.servers"] = client.cfg.Servers

	if key == "consumer" {
		cfgMap["group.id"] = client.cfg.Consumer.Group
		cfgMap["auto.offset.reset"] = client.cfg.Consumer.Start
	}

	if key == "producer" {
		// Load producer configuration: client.cfg.Producer
	}

	if client.cfg.AuthRequired {
		cfgMap["security.protocol"] = client.cfg.Auth.SecurityProtocol
		cfgMap["sasl.mechanism"] = client.cfg.Auth.Mechanism
		cfgMap["username"] = client.cfg.Auth.User
		cfgMap["password"] = client.cfg.Auth.Password
	}

	if client.cfg.Extras.JaasConfig != "" {
		cfgMap["sasl.jaas.config"] = client.cfg.Extras.JaasConfig
	}

	log.Printf("Kafka Config: %v\n", cfgMap)

	return cfgMap
}

func NewKafkaClient(ctx context.Context, p *KafkaConfig) MessageBusClient {
	return &KafkaClient{p}
}
