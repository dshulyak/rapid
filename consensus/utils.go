package consensus

import (
	"bytes"

	"github.com/dshulyak/rapid/consensus/types"
)

func newAggregate(qsize int) *aggregate {
	return &aggregate{
		qsize: qsize,
		from:  map[uint64]struct{}{},
	}
}

type aggregateValue struct {
	value *types.Value
	votes []uint64
}

// aggregate can be used to aggregate distinct messages
type aggregate struct {
	qsize   int // quorum size
	from    map[uint64]struct{}
	highest uint64

	values []aggregateValue
}

func (a *aggregate) add(from uint64, ballot uint64, value *types.Value) {
	if _, exist := a.from[from]; exist {
		return
	}
	a.from[from] = struct{}{}
	if ballot < a.highest {
		return
	}
	a.highest = ballot
	for i := range a.values {
		val := &a.values[i]
		if val.value == nil || bytes.Compare(val.value.Id, value.Id) == 0 {
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
		if count > a.qsize/2 {
			return val.value, false
		}
	}
	return nil, false
}

func (a *aggregate) any() *types.Value {
	for i := range a.values {
		val := &a.values[i]
		if !IsAny(val.value) {
			return val.value
		}
	}
	return nil
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

func newQueue() *queue {
	return &queue{
		items: map[string]*types.Value{},
	}
}

// queue is in-memory non thread safe fifo queue with ability to remove value by id from queue.
// TODO it can be persisted in order not to lose values submitted by client, that are either pending or proposed.
type queue struct {
	order []string
	items map[string]*types.Value
}

func (q *queue) add(item *types.Value) {
	id := string(item.Id)
	q.order = append(q.order, id)
	q.items[id] = item
}

func (q *queue) pop() *types.Value {
	for {
		if len(q.order) == 0 {
			return nil
		}
		id := q.order[0]
		copy(q.order, q.order[1:])
		last := len(q.order) - 1
		q.order = q.order[:last]
		item, exist := q.items[id]
		if exist {
			delete(q.items, id)
			return item
		}
	}
}

func (q *queue) remove(id []byte) {
	delete(q.items, string(id))
}

func (q *queue) empty() bool {
	return len(q.items) == 0
}
