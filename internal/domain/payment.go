package domain

import "fmt"

const (
	StatusCreated   = "created"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
)

func StatusForEvent(eventType string) (string, error) {
	switch eventType {
	case EventPaymentCreated:
		return StatusCreated, nil
	case EventPaymentSucceeded:
		return StatusSucceeded, nil
	case EventPaymentFailed:
		return StatusFailed, nil
	default:
		return "", fmt.Errorf("неизвестный тип события %q", eventType)
	}
}

// CanTransition описывает конечный автомат: created -> succeeded|failed.
func CanTransition(from, to string) bool {
	if from == to {
		return true
	}
	return from == StatusCreated && (to == StatusSucceeded || to == StatusFailed)
}
