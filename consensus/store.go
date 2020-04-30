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

type StoreSession interface {
	Store
	End() error
}

type TransactionalStore interface {
	Store
	StartSession() (StoreSession, error)
}

type Store interface {
	GetBallot() (uint64, error)
	SetBallot(uint64) error

	AddValues(...*types.LearnedValue) error
	CommitValue(*types.LearnedValue) error
	CommitedSequence() (uint64, error)
	GetValue(uint64) (*types.LearnedValue, error)

	UpdateCommited(uint64, uint64) error
	GetCommited(uint64) (uint64, error)
}
