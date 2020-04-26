package consensus

import (
	"context"
	"errors"
	"fmt"

	"github.com/dshulyak/rapid/consensus/types"
)

var (
	// ErrNotFound must be raised by persist backend if value is not found in the store.
	ErrNotFound = errors.New("not found")
)

func checkPersist(err error) {
	if err != nil {
		panic(fmt.Sprintf("persistence error: %v", err))
	}
}

type Persistence interface {
	GetBallot(context.Context) (uint64, error)
	SetBallot(context.Context, uint64) error

	AddLogs(context.Context, []*types.LearnedValue) error
	CommitLogs(context.Context, []*types.LearnedValue) error
	UpdateLastLogCommited(context.Context, uint64) error
	LastLogCommited(context.Context) (uint64, error)
	GetLog(context.Context, uint64) (*types.LearnedValue, error)
	GetLogs(context.Context, uint64, uint64) ([]*types.LearnedValue, error)

	UpdateCommited(context.Context, uint64, uint64) error
	GetCommited(context.Context, uint64) (uint64, error)

	BeginSession() error
	EndSession() error
}
