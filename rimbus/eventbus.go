package rimbus

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"strings"
	"time"
)

type MessageBusClient interface {
	Connect()
	Consume(topic string) (<-chan *MessageEvent, error)
	GetDSN() string
	GetEngine() interface{}
	Publish(topic string, message *MessageEvent) error
}

type MessageEvent struct {
	Action      string         `json:"action,omitempty"`
	Application string         `json:"application,omitempty"`
	Event       string         `json:"event,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
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
