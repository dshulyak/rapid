package consensus

import (
	"github.com/dshulyak/rapid/types"
)

func newAggregate(qsize, safety int) *aggregate {
	return &aggregate{
		qsize:  qsize,
		safety: safety,
		from:   map[uint64]struct{}{},
	}
}

type aggregateValue struct {
	value *types.Value
	votes []uint64
}

// aggregate can be used to aggregate distinct messages
type aggregate struct {
	qsize   int // quorum size
	safety  int // safety threshold (half of the fast quorum)
	from    map[uint64]struct{}
	highest uint64

	values []aggregateValue
}

func (a *aggregate) add(from uint64, ballot uint64, value *types.Value) {
	if _, exist := a.from[from]; exist {
		return
	}
	a.from[from] = struct{}{}
	if value == nil {
		return
	}
	if ballot < a.highest {
		return
	}
	a.highest = ballot
	for i := range a.values {
		val := &a.values[i]
		if val.value == nil || types.EqualValues(val.value, value) {
			val.votes = append(val.votes, ballot)
		}
	}
	a.values = append(a.values, aggregateValue{
		value: value,
		votes: []uint64{ballot},
	})
}

// safe returns safe value according to fast paxos selection rule:
// if only one value is picked by majority in the highest round - value is safe.
// if majority in the quorum picked same value in the highest round - value is safe.
// otherwise any value is safe and this method will return nil.
func (a *aggregate) safe() (*types.Value, bool) {
	for i := range a.values {
		count := 0
		val := &a.values[i]
		for _, vote := range val.votes {
			if vote == a.highest {
				count++
			}
		}
		if count == a.qsize {
			return val.value, true
		}
		if count > a.safety {
			return val.value, false
		}
	}
	return nil, false
}

func (a *aggregate) any() *types.Value {
	for i := range a.values {
		val := &a.values[i]
		return val.value
	}
	return nil
}

func (a *aggregate) iterateVoters(f func(uint64) bool) {
	for id := range a.from {
		if !f(id) {
			return
		}
	}
}

func (a *aggregate) iterate(f func(*types.Value) bool) {
	for _, v := range a.values {
		if !f(v.value) {
			return
		}
	}
}

func (a *aggregate) complete() bool {
	return len(a.from) >= a.qsize
}
