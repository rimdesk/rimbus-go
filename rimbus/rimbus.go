package rimbus

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	_ "github.com/joho/godotenv/autoload"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageBusClient interface {
	EstablishConnection()
	Consume(topic string, options *ConsumeOptions) (<-chan *MessageEvent, error)
	GetDSN() string
	GetEngine() interface{}
	Publish(topic string, message []byte, options *PublishOptions) (bool, error)
}

type BusEventResponse struct {
}

func (b BusEventResponse) String() string {
	jb, _ := json.Marshal(b)
	return string(jb)
}

type MessageEvent struct {
	Action       string            `json:"action,omitempty"`
	Application  string            `json:"application,omitempty"`
	Event        string            `json:"event,omitempty"`
	Metadata     map[string]any    `json:"metadata,omitempty"`
	Payload      []byte            `json:"payload,omitempty"`
	Timestamp    int64             `json:"timestamp,omitempty"`
	Acknowledger amqp.Acknowledger `json:"-"`
	Tag          uint64            `json:"-"`
}

type PublishOptions struct {
	Args        map[string]interface{} `json:"args,omitempty"`
	AutoDelete  bool                   `json:"auto_delete,omitempty"`
	ContentType string                 `json:"content_type,omitempty"`
	Durable     bool                   `json:"durable,omitempty"`
	Exclusive   bool                   `json:"exclusive,omitempty"`
	Exchange    string                 `json:"exchange,omitempty"`
	Mandatory   bool                   `json:"mandatory,omitempty"`
	NoWait      bool                   `json:"no_wait,omitempty"`
	Immediately bool                   `json:"immediately,omitempty"`
}

type ConsumeOptions struct {
	Args         map[string]interface{} `json:"args,omitempty"`
	AutoAck      bool                   `json:"auto_ack,omitempty"`
	AutoDelete   bool                   `json:"auto_delete,omitempty"`
	ConsumerName string                 `json:"consumer_name,omitempty"`
	Exchange     string                 `json:"exchange,omitempty"`
	QueueName    string                 `json:"queue_name,omitempty"`
	Durable      bool                   `json:"durable,omitempty"`
	Exclusive    bool                   `json:"exclusive,omitempty"`
	NoLocal      bool                   `json:"no_local,omitempty"`
	NoWait       bool                   `json:"no_wait,omitempty"`
}

func New(ctx context.Context) MessageBusClient {
	config := getConfig()
	switch config.Default.Engine {
	default:
		return NewRabbitMQClient(ctx, &config.RabbitMQ)
	}
}

// DefaultConfig holds the default settings.
type DefaultConfig struct {
	Engine string `mapstructure:"engine"`
}

// KafkaAuth holds authentication details.
type KafkaAuth struct {
	SecurityProtocol string `mapstructure:"sasl_security_protocol"`
	Mechanism        string `mapstructure:"sasl_mechanism"`
	Username         string `mapstructure:"username"`
	Password         string `mapstructure:"password"`
}

// KafkaExtras holds additional settings.
type KafkaExtras struct {
	JaasConfig string `mapstructure:"jaas_config"`
}

// KafkaConsumer holds consumer settings.
type KafkaConsumer struct {
	Group string `mapstructure:"group"`
	Start string `mapstructure:"start"`
}

// KafkaConfig holds Kafka settings.
type KafkaConfig struct {
	Servers      string        `mapstructure:"servers"`
	AuthRequired bool          `mapstructure:"auth_required"`
	Topics       []string      `mapstructure:"topics"`
	Auth         KafkaAuth     `mapstructure:"auth"`
	Extras       KafkaExtras   `mapstructure:"extras"`
	Consumer     KafkaConsumer `mapstructure:"consumer"`
}

// RabbitMqAuth holds RabbitMQ authentication.
type RabbitMqAuth struct {
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

// RabbitMQConfig holds RabbitMQ configuration.
type RabbitMQConfig struct {
	Host     string       `mapstructure:"host"`
	Port     string       `mapstructure:"port"`
	Exchange string       `mapstructure:"exchange"`
	Protocol string       `mapstructure:"protocol"`
	Auth     RabbitMqAuth `mapstructure:"auth"`
}

// Config represents the entire configuration.
type Config struct {
	Default  DefaultConfig  `mapstructure:"default"`
	Kafka    KafkaConfig    `mapstructure:"kafka"`
	RabbitMQ RabbitMQConfig `mapstructure:"rabbitmq"`
}

func getConfig() *Config {
	cfg := Config{
		Default: DefaultConfig{
			Engine: os.Getenv("ENGINE"),
		},
		RabbitMQ: RabbitMQConfig{
			Host:     os.Getenv("RABBITMQ_HOST"),
			Port:     os.Getenv("RABBITMQ_PORT"),
			Exchange: os.Getenv("RABBITMQ_EXCHANGE"),
			Protocol: os.Getenv("RABBITMQ_SCHEME"),
			Auth: RabbitMqAuth{
				Username: os.Getenv("RABBITMQ_AUTH_USERNAME"),
				Password: os.Getenv("RABBITMQ_AUTH_PASSWORD"),
			},
		},
	}
	// Print out the parsed values
	log.Printf("[💨💨] Rimbus Engine Loaded: '%s' [💨💨]\n", cfg.Default.Engine)

	return &cfg
}

func NewEvent(application, event, action string) *MessageEvent {
	return &MessageEvent{
		Action:      action,
		Application: application,
		Event:       event,
		Metadata:    nil,
		Payload:     nil,
		Timestamp:   time.Now().UnixMilli(),
	}
}

func NewEventWithPayload(application, event, action string, payload []byte) *MessageEvent {
	return &MessageEvent{
		Action:      action,
		Application: application,
		Event:       event,
		Metadata:    nil,
		Payload:     payload,
		Timestamp:   time.Now().UnixMilli(),
	}
}

func (e MessageEvent) String() string {
	jb, _ := json.Marshal(e)
	return string(jb)
}

const (
	Prod = "production"
	Dev  = "development"
)
