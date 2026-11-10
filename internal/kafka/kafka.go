package kafka

import (
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const BatchTimeout = 20 * time.Millisecond

func NewWriter(brokers []string, topic string) *kafkago.Writer {
	return &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{},
		BatchTimeout: BatchTimeout,
		RequiredAcks: kafkago.RequireAll,
		Async:        false,
	}
}

func ValidateBrokers(brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS не задан")
	}
	return nil
}
