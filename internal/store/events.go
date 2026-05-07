package store

import (
	"errors"
	"time"
)

var ErrEventNotClaimable = errors.New("event is not claimable")
var ErrEventLateFinalizer = errors.New("event finalizer lost lease")

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
