package consensus

import (
	"github.com/dshulyak/rapid/types"
)

// Application doesn't need to store log of values, it is enough to ensure that last agreed value is replicated.
func newValues() *values {
	return &values{values: map[uint64]*types.LearnedValue{}}
}

type values struct {
	// sequence is the last commited sequence number.
	sequence uint64
	values   map[uint64]*types.LearnedValue
}

// Add expects values to be provided in order.
func (vs *values) add(values ...*types.LearnedValue) {
	for _, v := range values {
		vs.values[v.Sequence] = v
	}
}

// Commit expects values to be provided in order.
func (vs *values) commit(value *types.LearnedValue) {
	if value.Sequence <= vs.sequence {
		return
	}
	for i := range vs.values {
		delete(vs.values, i)
	}
	vs.add(value)
	vs.sequence = value.Sequence
}

func (vs *values) get(seq uint64) *types.LearnedValue {
	return vs.values[seq]
}

func (vs *values) commited() uint64 {
	return vs.sequence
}
