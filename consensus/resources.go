package consensus

import (
	"context"
	"errors"
	"time"

	"github.com/dshulyak/rapid/consensus/types"
)

var (
	ptimeout = 3 * time.Second
)

// TODO Ballot/Log/CommitedState doesn't have to be exported

type Ballot struct {
	store Persistence
	value uint64
}

func (bal *Ballot) Get() uint64 {
	return bal.value
}

func (bal *Ballot) Set(value uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), ptimeout)
	defer cancel()
	checkPersist(bal.store.SetBallot(ctx, value))
	bal.value = value
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
	checkPersist(l.store.AddLogs(context.TODO(), values))
}

// Commit expects values to be provided in order.
func (l *Log) Commit(values ...*types.LearnedValue) {
	if len(values) == 0 {
		return
	}
	checkPersist(l.store.CommitLogs(context.TODO(), values))
	last := values[len(values)-1]
	if last.Sequence > l.last {
		checkPersist(l.store.UpdateLastLogCommited(context.TODO(), last.Sequence))
		l.last = last.Sequence
	}
}

func (l *Log) Get(seq uint64) *types.LearnedValue {
	value, err := l.store.GetLog(context.TODO(), seq)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	checkPersist(err)
	return value
}

func (l *Log) List(lo, hi uint64) []*types.LearnedValue {
	values, err := l.store.GetLogs(context.TODO(), lo, hi)
	checkPersist(err)
	return values
}

func (l *Log) Commited() uint64 {
	return l.last
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
	checkPersist(cs.store.UpdateCommited(context.TODO(), replica, seq))
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
