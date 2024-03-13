package eventbus

import (
	"github.com/rimdesk/eventbus-go/thirdparty/kafka"
	"github.com/rimdesk/eventbus-go/thirdparty/rabbitmq"
)

// Inventory Events
const (
	InventoryCreatedAction         string = "inventory.created"
	InventoryCreateAction                 = "inventory.create"
	InventoryDeleteAction                 = "inventory.delete"
	InventoryDeletedAction                = "inventory.deleted"
	InventoryTrashedAction                = "inventory.trashed"
	InventoryQuantityChangedAction        = "inventory.quantity_changed"
)

// Product Events
const (
	ProductCreateAction  string = "product.create"
	ProductCreatedAction        = "product.created"
	ProductTrashedAction        = "product.trashed"
	ProductDeleteAction         = "product.delete"
	ProductDeletedAction        = "product.deleted"
)

func NewKafkaClient() MessageBusClient {
	return &kafka.KClient{}
}

func NewRabbitMQClient() MessageBusClient {
	return &rabbitmq.MqClient{}
}
