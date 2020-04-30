package consensus

import (
	"errors"

	"github.com/dshulyak/rapid/consensus/types"
)

// TODO Ballot/Log/CommitedState doesn't have to be exported

func NewBallot(store Store) *Ballot {
	ballot, err := store.GetBallot()
	if err != nil && errors.Is(err, ErrNotFound) {
		checkPersist(err)
	}
	return &Ballot{store: store, value: ballot}
}

type Ballot struct {
	store Store
	value uint64
}

func (bal *Ballot) WithStore(store Store) *Ballot {
	bal.store = store
	return bal
}

func (bal *Ballot) Get() uint64 {
	return bal.value
}

func (bal *Ballot) Set(value uint64) {
	checkPersist(bal.store.SetBallot(value))
	bal.value = value
}

// TODO rename from Log to something more appropriate
// Application doesn't need to store log of values, it is enough to ensure that last agreed value is replicated.
func NewLog(store Store) *Log {
	last, err := store.CommitedSequence()
	if err != nil && errors.Is(err, ErrNotFound) {
		checkPersist(err)
	}
	return &Log{store: store, last: last}
}

type Log struct {
	store Store
	last  uint64
}

func (l *Log) WithStore(store Store) *Log {
	l.store = store
	return l
}

// Add expects values to be provided in order.
func (l *Log) Add(values ...*types.LearnedValue) {
	if len(values) == 0 {
		return
	}
	checkPersist(l.store.AddValues(values...))
}

// Commit expects values to be provided in order.
func (l *Log) Commit(value *types.LearnedValue) {
	if value.Sequence <= l.last {
		return
	}
	checkPersist(l.store.CommitValue(value))
	l.last = value.Sequence
}

func (l *Log) Get(seq uint64) *types.LearnedValue {
	value, err := l.store.GetValue(seq)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	checkPersist(err)
	return value
}

func (l *Log) Commited() uint64 {
	return l.last
}

func NewCommitedState(store Store, replicas []uint64) *CommitedState {
	state := &CommitedState{
		store:  store,
		values: map[uint64]uint64{},
	}
	for _, r := range replicas {
		value, err := store.GetCommited(r)
		if err != nil && !errors.Is(err, ErrNotFound) {
			checkPersist(err)
		}
		state.values[r] = value
	}
	return state
}

// CommitedState maintained by the coordinator for each replica.
// It is coordinators reponsibility to share learned values.
type CommitedState struct {
	store  Store
	values map[uint64]uint64
}

func (cs *CommitedState) WithStore(store Store) *CommitedState {
	cs.store = store
	return cs
}

func (cs *CommitedState) Get(replica uint64) uint64 {
	val, exist := cs.values[replica]
	if !exist {
		return 0
	}
	return val
}

func (cs *CommitedState) Update(replica, seq uint64) {
	cs.values[replica] = seq
	checkPersist(cs.store.UpdateCommited(replica, seq))
}
