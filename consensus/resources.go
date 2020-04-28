package consensus

import (
	"errors"

	"github.com/dshulyak/rapid/consensus/types"
)

// TODO Ballot/Log/CommitedState doesn't have to be exported

func NewBallot(store Persistence) *Ballot {
	ballot, err := store.GetBallot()
	if err != nil && errors.Is(err, ErrNotFound) {
		checkPersist(err)
	}
	return &Ballot{store: store, value: ballot}
}

type Ballot struct {
	store Persistence
	value uint64
}

func (bal *Ballot) Get() uint64 {
	return bal.value
}

func (bal *Ballot) Set(value uint64) {
	checkPersist(bal.store.SetBallot(value))
	bal.value = value
}

func NewLog(store Persistence) *Log {
	last, err := store.LastLogCommited()
	if err != nil && errors.Is(err, ErrNotFound) {
		checkPersist(err)
	}
	return &Log{store: store, last: last}
}

type Log struct {
	store Persistence
	last  uint64
}

// Add expects values to be provided in order.
func (l *Log) Add(values ...*types.LearnedValue) {
	if len(values) == 0 {
		return
	}
	checkPersist(l.store.AddLogs(values))
}

// Commit expects values to be provided in order.
func (l *Log) Commit(values ...*types.LearnedValue) {
	if len(values) == 0 {
		return
	}
	checkPersist(l.store.CommitLogs(values))
	last := values[len(values)-1]
	if last.Sequence > l.last {
		checkPersist(l.store.UpdateLastLogCommited(last.Sequence))
		l.last = last.Sequence
	}
}

func (l *Log) Get(seq uint64) *types.LearnedValue {
	value, err := l.store.GetLog(seq)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	checkPersist(err)
	return value
}

func (l *Log) List(lo, hi uint64) []*types.LearnedValue {
	values, err := l.store.GetLogs(lo, hi)
	checkPersist(err)
	return values
}

func (l *Log) Commited() uint64 {
	return l.last
}

func NewCommitedState(store Persistence, replicas []uint64) *CommitedState {
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
	store  Persistence
	values map[uint64]uint64
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
