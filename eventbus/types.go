package eventbus

import "time"

type MessageBusClient interface {
	Connect()
	GetDSN() string
	GetEngine() interface{}
	Publish(topic string, message interface{}) error
	Subscribe(topic string) (<-chan MessageEvent, error)
}

type MessageEvent struct {
	Application string         `json:"application,omitempty"`
	Topic       string         `json:"topic,omitempty"`
	Action      string         `json:"action,omitempty"`
	Timestamp   int64          `json:"timestamp,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type BrokerParams struct {
}

type ConfigParams struct {
	Engine string        `json:"engine,omitempty"`
	Params *BrokerParams `json:"params,omitempty"`
}

func NewMessageEvent(topic, subscription, action string) *MessageEvent {
	return &MessageEvent{
		Topic:     topic,
		Timestamp: time.Now().UnixMilli(),
		Payload:   nil,
	}
}
