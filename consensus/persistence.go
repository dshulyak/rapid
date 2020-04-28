package consensus

import (
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
	GetBallot() (uint64, error)
	SetBallot(uint64) error

	AddLogs([]*types.LearnedValue) error
	CommitLogs([]*types.LearnedValue) error
	UpdateLastLogCommited(uint64) error
	LastLogCommited() (uint64, error)
	GetLog(uint64) (*types.LearnedValue, error)
	GetLogs(uint64, uint64) ([]*types.LearnedValue, error)

	UpdateCommited(uint64, uint64) error
	GetCommited(uint64) (uint64, error)

	BeginSession() error
	EndSession() error
}
