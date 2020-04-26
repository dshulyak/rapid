package consensus

import "github.com/dshulyak/rapid/consensus/types"

type Ballot interface {
	Get() uint64
	Set(ballot uint64)
}

type Log interface {
	Add(...*types.LearnedValue)
	Commit(...*types.LearnedValue)
	Get(seq uint64) *types.LearnedValue
	// TODO List should returnd iterator.
	List(lo, hi uint64) []*types.LearnedValue
	// Commited returns sequence of the last commited value.
	Commited() uint64
}

// CommitedState maintained by the coordinator for each replica.
// It is coordinators reponsibility to share learned values.
type CommitedState interface {
	Update(replica, seq uint64)
	Get(uint64) uint64
}

func newAggregate(qsize int) *aggregate {
	return &aggregate{
		qsize: qsize,
		from:  map[uint64]struct{}{},
	}
}

// aggregate can be used to aggregate distinct messages
type aggregate struct {
	qsize    int // quorum size
	from     map[uint64]struct{}
	messages []*types.Message
}

func (a *aggregate) add(msg MessageFrom) {
	if _, exist := a.from[msg.From]; exist {
		return
	}
	a.from[msg.From] = struct{}{}
	a.messages = append(a.messages, msg.Message)
}

func (a *aggregate) complete() bool {
	return len(a.messages) >= a.qsize
}

func (a *aggregate) iterate(f func(*types.Message) bool) {
	for _, msg := range a.messages {
		if !f(msg) {
			return
		}
	}
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
