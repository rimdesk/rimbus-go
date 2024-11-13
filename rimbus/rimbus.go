package rimbus

import (
	"context"
	"encoding/json"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/spf13/viper"
	"log"
	"strings"
	"time"
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

func New(ctx context.Context, environment string) MessageBusClient {
	config := getConfig(environment)
	switch strings.ToLower(config.Default.Engine) {
	case "kafka":
		return NewKafkaClient(ctx, config.Kafka)
	default:
		return NewRabbitMQClient(ctx, config.RabbitMQ)
	}
}

type DefaultConfig struct {
	Engine string `mapstructure:"engine"`
}

type KafkaAuth struct {
	SecurityProtocol string `mapstructure:"sasl_security_protocol"`
	Mechanism        string `mapstructure:"sasl_mechanism"`
	User             string `mapstructure:"username"`
	Password         string `mapstructure:"password"`
}

type KafkaExtras struct {
	JaasConfig string `mapstructure:"jaas_config"`
}

type KafkaConfig struct {
	Servers      string   `mapstructure:"servers"`
	AuthRequired bool     `mapstructure:"auth_required"`
	Producer     struct{} // Empty struct to handle empty sections
	Consumer     struct {
		Group string `mapstructure:"group"`
		Start string `mapstructure:"start"`
	} `mapstructure:"consumer"`
	Auth   *KafkaAuth `mapstructure:"auth"`
	Extras struct {
		JaasConfig string
	} `mapstructure:"auth"`
}

type RabbitMqAuth struct {
	User     string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type RabbitMQConfig struct {
	Host     string        `mapstructure:"host"`
	Port     string        `mapstructure:"port"`
	Queue    string        `mapstructure:"queue"`
	Protocol string        `mapstructure:"protocol"`
	Auth     *RabbitMqAuth `mapstructure:"auth"`
}

// Config represents the TOML configuration structure
type Config struct {
	Default  *DefaultConfig  `mapstructure:"default"`
	Kafka    *KafkaConfig    `mapstructure:"kafka"`
	RabbitMQ *RabbitMQConfig `mapstructure:"rabbitmq"`
}

func getConfig(environment string) *Config {
	// Set the file name (config.toml) and path (current directory)
	if strings.ToLower(environment) != "development" {
		viper.SetConfigName("config")
	} else {
		viper.SetConfigName("config.dev")
	}

	viper.SetConfigType("toml")
	viper.AddConfigPath(".")

	// Read the TOML configuration file
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("‼️Error reading configuration: %v", err)
	}

	// Define a variable to store the configuration
	var cfg Config

	// Unmarshal the configuration into the struct
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatalf("‼️Error unmarshalling config: %v", err)
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
