package consensus

import (
	"github.com/dshulyak/rapid/consensus/types"
)

// TODO rename from Log to something more appropriate
// Application doesn't need to store log of values, it is enough to ensure that last agreed value is replicated.
func NewLog() *Log {
	return &Log{values: map[uint64]*types.LearnedValue{}}
}

type Log struct {
	commited uint64
	values   map[uint64]*types.LearnedValue
}

// Add expects values to be provided in order.
func (l *Log) Add(values ...*types.LearnedValue) {
	for _, v := range values {
		l.values[v.Sequence] = v
	}
}

// Commit expects values to be provided in order.
func (l *Log) Commit(value *types.LearnedValue) {
	if value.Sequence <= l.commited {
		return
	}
	l.Add(value)
	l.commited = value.Sequence
}

func (l *Log) Get(seq uint64) *types.LearnedValue {
	val, _ := l.values[seq]
	return val
}

func (l *Log) Commited() uint64 {
	return l.commited
}

func NewCommitedState(replicas []uint64) *CommitedState {
	return &CommitedState{
		values: map[uint64]uint64{},
	}
}

// CommitedState maintained by the coordinator for each replica.
// It is coordinators reponsibility to share learned values.
type CommitedState struct {
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
}
