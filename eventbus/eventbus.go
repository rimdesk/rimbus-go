package eventbus

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strings"
	"time"
)

type MessageBusClient interface {
	Connect()
	GetDSN() string
	GetEngine() interface{}
	Publish(topic string, message *MessageEvent) error
	Consume(topic string) (<-chan *MessageEvent, error)
}

type MessageEvent struct {
	Action      string         `json:"action,omitempty"`
	Application string         `json:"application,omitempty"`
	Event       string         `json:"event,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Timestamp   int64          `json:"timestamp,omitempty"`
}

type BrokerParams struct {
	Connection  string           `json:"connection,omitempty"`
	KafkaConfig *kafka.ConfigMap `json:"configMap,omitempty"`
}

type ConfigParams struct {
	Engine string        `json:"engine,omitempty"`
	Params *BrokerParams `json:"params,omitempty"`
}

func New(params *ConfigParams) MessageBusClient {
	switch strings.ToLower(params.Engine) {
	case "kafka":
		return NewKafkaClient(params.Params)
	default:
		return NewRabbitMQClient(params.Params)
	}
}

func NewEvent(application, event, action string) *MessageEvent {
	return &MessageEvent{
		Action:      action,
		Application: application,
		Event:       event,
		Payload:     nil,
		Metadata:    nil,
		Timestamp:   time.Now().UnixMilli(),
	}
}

func NewEventWithPayload(application, event, action string, payload map[string]any) *MessageEvent {
	return &MessageEvent{
		Action:      action,
		Application: application,
		Event:       event,
		Payload:     payload,
		Metadata:    nil,
		Timestamp:   time.Now().UnixMilli(),
	}
}

func (e MessageEvent) String() string {
	jb, _ := json.Marshal(e)
	return string(jb)
}

const (
	AppAccountTopic     string = "rimdesk.account"
	AppAccountingTopic         = "rimdesk.accounting"
	AppCompanyTopic            = "rimdesk.company"
	AppHRTopic                 = "rimdesk.hr"
	AppInventoryTopic          = "rimdesk.inventory"
	AppProcurementTopic        = "rimdesk.procurement"
	AppProductTopic            = "rimdesk.product"
	AppWarehouseTopic          = "rimdesk.warehouse"
)

const (
	InventoryCreateEvent        string = "inventory.create"
	InventoryCreatedEvent              = "inventory.created"
	InventoryDeleteEvent               = "inventory.delete"
	InventoryDeletedEvent              = "inventory.deleted"
	InventoryChangedEvent              = "inventory.changed"
	InventoryTrashedEvent              = "inventory.trashed"
	InventoryExportEvent               = "inventory.exported"
	InventoryImportEvent               = "inventory.imported"
	InventoryStockCreatedEvent         = "inventory.stock.created"
	InventoryStockTransferEvent        = "inventory.stock.transfer"
	InventoryStockModifiedEvent        = "inventory.stock.modified"
	InventoryStockReceivedEvent        = "inventory.stock.received"
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
	SupplierCreatedEvent  string = "supplier.created"
	SupplierApprovedEvent        = "supplier.approved"
	SupplierRejectedEvent        = "supplier.rejected"
)

const (
	PurchaseOrderCreatedEvent  string = "purchase_order.created"
	PurchaseOrderReceivedEvent        = "purchase_order.received"
	PurchaseOrderApprovedEvent        = "purchase_order.approved"
	PurchaseOrderRejectedEvent        = "purchase_order.rejected"
)
