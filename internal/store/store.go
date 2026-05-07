// Package store defines queue and bookkeeping storage interfaces.
package store

import "errors"

var (
	ErrNotOwner  = errors.New("job is not owned by runner instance")
	ErrLostLease = errors.New("job lease is no longer valid")
)

type QueueStore interface {
	Close() error
}
