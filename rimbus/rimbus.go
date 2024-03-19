package rimbus

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spf13/viper"
	"log"
	"strings"
	"time"
)

type MessageBusClient interface {
	Connect()
	Consume(topic string) (<-chan *MessageEvent, error)
	GetDSN() string
	GetEngine() interface{}
	Publish(topic string, message *MessageEvent) (chan kafka.Event, error)
}

type MessageEvent struct {
	Action      string         `json:"action,omitempty"`
	Application string         `json:"application,omitempty"`
	Event       string         `json:"event,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
	Timestamp   int64          `json:"timestamp,omitempty"`
}

func New() MessageBusClient {
	config := getConfig()
	switch strings.ToLower(config.Default.Engine) {
	case "kafka":
		return NewKafkaClient(config.Kafka)
	default:
		return NewRabbitMQClient(config.RabbitMQ)
	}
}

type DefaultConfig struct {
	Engine string `mapstructure:"engine"`
}

type KafkaConfig struct {
	Servers  string   `mapstructure:"servers"`
	Producer struct{} // Empty struct to handle empty sections
	Consumer struct {
		Group string `mapstructure:"group"`
		Start string `mapstructure:"start"`
	} `mapstructure:"consumer"`
}

type RabbitMQConfig struct {
	Host     string `mapstructure:"host"`
	Topic    string `mapstructure:"topic"`
	Exchange string `mapstructure:"exchange"`
}

// Config represents the TOML configuration structure
type Config struct {
	Default  *DefaultConfig  `mapstructure:"default"`
	Kafka    *KafkaConfig    `mapstructure:"kafka"`
	RabbitMQ *RabbitMQConfig `mapstructure:"rabbitmq"`
}

func getConfig() *Config {
	// Set the file name (config.toml) and path (current directory)
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")

	// Read the TOML configuration file
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("‼️Error reading config file: %v", err)
	}

	// Define a variable to store the configuration
	var cfg Config

	// Unmarshal the configuration into the struct
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Fatalf("‼️Error unmarshalling config: %v", err)
	}

	// Print out the parsed values
	log.Printf("Engine: %s\n", cfg.Default.Engine)

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

func NewEventWithPayload(application, event, action string, payload map[string]any) *MessageEvent {
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
	AppAccountTopic       string = "rimdesk.account"
	AppAccountingTopic           = "rimdesk.accounting"
	AppCompanyTopic              = "rimdesk.company"
	AppHRTopic                   = "rimdesk.hr"
	AppInventoryTopic            = "rimdesk.inventory"
	AppProcurementTopic          = "rimdesk.procurement"
	AppProductTopic              = "rimdesk.product"
	AppProfileTopic              = "rimdesk.profile"
	AppPurchaseOrderTopic        = "rimdesk.purchase_order"
	AppSupplierTopic             = "rimdesk.supplier"
	AppWarehouseTopic            = "rimdesk.warehouse"
)

const (
	InventoryChangedEvent       string = "inventory.changed"
	InventoryCreateEvent               = "inventory.create"
	InventoryCreatedEvent              = "inventory.created"
	InventoryDeleteEvent               = "inventory.delete"
	InventoryDeletedEvent              = "inventory.deleted"
	InventoryExportEvent               = "inventory.exported"
	InventoryImportEvent               = "inventory.imported"
	InventoryStockCreatedEvent         = "inventory.stock.created"
	InventoryStockModifiedEvent        = "inventory.stock.modified"
	InventoryStockReceivedEvent        = "inventory.stock.received"
	InventoryStockTransferEvent        = "inventory.stock.transfer"
	InventoryTrashedEvent              = "inventory.trashed"
)

const (
	WarehouseCreateEvent  string = "warehouse.create"
	WarehouseCreatedEvent        = "warehouse.created"
)

const (
	ProductCreateEvent  string = "product.create"
	ProductCreatedEvent        = "product.created"
	ProductDeleteEvent         = "product.delete"
	ProductDeletedEvent        = "product.deleted"
	ProductTrashedEvent        = "product.trashed"
)

const (
	ProfileCreatedEvent  string = "profile.created"
	ProfileVerifiedEvent        = "profile.verified"
)

const (
	SupplierApprovedEvent string = "supplier.approved"
	SupplierCreatedEvent         = "supplier.created"
	SupplierRejectedEvent        = "supplier.rejected"
)

const (
	PurchaseOrderApprovedEvent string = "purchase_order.approved"
	PurchaseOrderCreatedEvent         = "purchase_order.created"
	PurchaseOrderReceivedEvent        = "purchase_order.received"
	PurchaseOrderRejectedEvent        = "purchase_order.rejected"
)

const (
	AccountServiceApi string = "accounting-api"
	CompanyApi               = "company-api"
	HRApi                    = "hr-api"
	InventoryApi             = "inventory-api"
	ProcurementApi           = "procurement-api"
	ProductApi               = "product-api"
	ProfileApi               = "profile-api"
	PurchaseOrderApi         = "purchase-order-api"
	SupplierApi              = "supplier-api"
	WarehouseApi             = "warehouse-api"
)
