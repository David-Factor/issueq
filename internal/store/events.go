package store

import (
	"errors"
	"time"
)

var ErrEventLateFinalizer = errors.New("event finalizer lost lease")

type EventBlockReason struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type EventClaimOptions struct {
	RouteName     string
	LeaseOwner    string
	LeaseDuration time.Duration
	MaxAttempts   int
	Now           time.Time
}

type EventFinalize struct {
	Status     string
	ResultJSON string
	LeaseOwner string
	Now        time.Time
}
