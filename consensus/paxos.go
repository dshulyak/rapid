package consensus

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/dshulyak/rapid/consensus/types"
	"go.uber.org/zap"
)

var (
	any      = []byte("ANY")
	anyValue = &types.Value{Id: any}
)

func IsAny(value *types.Value) bool {
	return bytes.Compare(value.Id, any) == 0
}

type MessageTo struct {
	Message *types.Message
	To      []uint64
}

type MessageFrom struct {
	Message *types.Message
	From    uint64
}

type Config struct {
	Timeout   int
	ReplicaID uint64
	Quorum    int
	Replicas  []uint64
}

func newPaxos(logger *zap.SugaredLogger, store Persistence, conf Config) *paxos {
	replicas := map[uint64]struct{}{}
	for _, r := range conf.Replicas {
		replica[r] = struct{}{}
	}
	return &paxos{
		logger:             logger,
		store:              store,
		replicaID:          conf.ReplicaID,
		qsize:              conf.Quorum,
		timeout:            conf.Timeout,
		replicas:           replicas,
		promiseAggregates:  map[uint64]*aggregate{},
		acceptedAggregates: map[uint64]*aggregate{},
	}
}

// paxos is an implementation of fast multipaxos variant of the algorithm.
type paxos struct {
	logger *zap.SugaredLogger
	store  Persistence

	// configuration
	replicaID uint64
	qsize     int
	timeout   int

	// TODO add heartbeat timeout

	// TODO we need to have information if replica is currently reachable or not
	// to avoid generating/sending useless messages
	replicas map[uint64]struct{}

	// volatile state
	ticks int
	// promiseAggregate maps sequence to promise messages
	promiseAggregates  map[uint64]*aggregate
	acceptedAggregates map[uint64]*aggregate
	proposed           *queue

	// persistent state
	log    *Log
	ballot *Ballot
	// commited is not nil only for the coordinator
	commited *CommitedState
}

func (p *paxos) Tick() ([]MessageTo, error) {
	p.ticks++
	if p.ticks == p.timeout {
		p.ticks = 0
		return p.slowBallot(p.ballot.Get() + 1)
	}
	return nil, nil
}

func (p *paxos) Propose(value *types.Value) error {
	p.proposed.add(value)
	return nil
}

func (p *paxos) slowBallot(bal uint64) ([]MessageTo, error) {
	seq := p.log.Commited() + 1
	p.promiseAggregates[seq] = newAggregate(p.qsize)
	return []MessageTo{{Message: types.NewPrepareMessage(bal, seq)}}, nil
}

func (p *paxos) Step(msg MessageFrom) ([]MessageTo, error) {
	if prepare := msg.Message.GetPrepare(); prepare != nil {
		return p.stepPrepare(msg)
	}
	if promise := msg.Message.GetPromise(); promise != nil {
		return p.stepPromise(msg)
	}
	if accept := msg.Message.GetAccept(); accept != nil {
		return p.stepAccept(msg)
	}
	if accepted := msg.Message.GetAccepted(); accepted != nil {
		return p.stepAccepted(msg)
	}
	if learned := msg.Message.GetLearned(); learned != nil {
		return p.stepLearned(msg)
	}
	// TODO export unknown message error
	// can be used to blacklist replica with incompatible protocol
	return nil, errors.New("unknown message")
}

// TODO move to Acceptor role.
func (p *paxos) stepPrepare(msg MessageFrom) ([]MessageTo, error) {
	prepare := msg.Message.GetPrepare()
	if prepare.Ballot <= p.ballot.Get() {
		// return only a ballot for the sender to update himself
		return []MessageTo{
			{
				Message: types.NewFailedPromiseMessage(p.ballot.Get()),
				To:      []uint64{msg.From},
			},
		}, nil
	}
	p.ticks = 0
	p.ballot.Set(prepare.Ballot)
	learned := p.log.Get(prepare.Sequence)
	var (
		value *types.Value
		voted uint64
	)
	if learned != nil {
		value = learned.Value
		voted = learned.Ballot
	}
	return []MessageTo{{
		Message: types.NewPromiseMessage(
			p.ballot.Get(),
			prepare.Sequence,
			voted,
			p.log.Commited(),
			value,
		),
		To: []uint64{msg.From},
	}}, nil
}

// TODO stepPromise must be processed by Coordinator role.
func (p *paxos) stepPromise(msg MessageFrom) ([]MessageTo, error) {
	promise := msg.Message.GetPromise()
	if promise.Ballot > p.ballot.Get() {
		// TODO step down from leader role
		p.ballot.Set(promise.Ballot)
		p.ticks = 0
		return nil, nil
	}
	p.commited.Update(msg.From, promise.CommitedSequence)
	msgs, err := p.updateReplicas()
	if err != nil {
		return nil, err
	}

	agg, exist := p.promiseAggregates[promise.Sequence]
	if !exist {
		return nil, fmt.Errorf("unexpected promise. ballot %d. sequence %d", promise.Ballot, promise.Sequence)
	}
	seq := promise.Sequence
	agg.add(msg)
	if agg.complete() {
		var (
			max   uint64
			value *types.Value
		)
		agg.iterate(func(msg *types.Message) bool {
			// FIXME this rule isn't valid for fast paxos
			// change it after implementing poc
			promise := msg.GetPromise()
			if promise.Ballot > max {
				max = promise.Ballot
				value = promise.Value
			}
			return true
		})

		delete(p.promiseAggregates, seq)
		p.acceptedAggregates[seq] = newAggregate(p.qsize)
		if value == nil {
			value = anyValue
		}
		return append(msgs, MessageTo{
			Message: types.NewAcceptMessage(p.ballot.Get(), seq, value),
		}), nil
	}
	return nil, nil
}

func (p *paxos) stepAccept(msg MessageFrom) ([]MessageTo, error) {
	accept := msg.Message.GetAccept()
	if p.ballot.Get() > accept.Ballot {
		// it may return failed accepted message, but returning failed promise is
		// safe from protocol pov, and doesn't require new constructor.
		// the purpose is the same as in stepPrepare - for coordinator to update himself
		return []MessageTo{
			{
				Message: types.NewFailedPromiseMessage(p.ballot.Get()),
				To:      []uint64{msg.From},
			},
		}, nil
	}
	value := accept.Value
	if IsAny(value) {
		// pick value from proposed queue
		if p.proposed.empty() {
			return nil, nil
		}
		value = p.proposed.pop()
	}
	p.log.Add(&types.LearnedValue{
		Ballot:   accept.Ballot,
		Sequence: accept.Sequence,
		Value:    value,
	})
	return []MessageTo{
		{
			Message: types.NewAcceptedMessage(accept.Ballot, accept.Sequence, value),
			To:      []uint64{msg.From},
		},
	}, nil
}

func (p *paxos) stepAccepted(msg MessageFrom) ([]MessageTo, error) {
	accepted := msg.Message.GetAccepted()
	agg, exist := p.acceptedAggregates[accepted.Sequence]
	if !exist {
		return nil, nil
	}
	agg.add(msg)
	if agg.complete() {
		var (
			max   uint64
			value *types.Value
			count int
		)
		agg.iterate(func(msg *types.Message) bool {
			accepted := msg.GetAccepted()
			if accepted.Ballot > max {
				max = accepted.Ballot
				value = accepted.Value
			}
			if accepted.Ballot == max && value != nil && bytes.Compare(value.Id, accepted.Value.Id) == 0 {
				count++
			}
			return true
		})
		if count >= p.qsize {
			learned := &types.LearnedValue{
				Ballot:   max,
				Sequence: accepted.Sequence,
				Value:    value,
			}
			p.log.Commit(learned)

			var msgs []MessageTo
			if !p.skipPrepareAllowed() {
				start, err := p.slowBallot(max)
				if err != nil {
					return nil, err
				}
				msgs = append(msgs, start...)
			} else {
				msgs = append(msgs, MessageTo{
					Message: types.NewAcceptMessage(p.ballot.Get(), p.log.Commited()+1, anyValue),
				})
			}

			updates, err := p.updateReplicas()
			if err != nil {
				return nil, err
			}

			return append(msgs, updates...), nil
		}
		// coordinated recovery

		// according to simplified selection rule (from paxos made easy paper)
		// value is safe if it has been upvoted by majority of voters in a quorum (e.g. atleast half of qsize)
		// otherwise any value is safe
		// in our case we guarantee if there is such a value that was upvoted by majority it will be selected
		// otherwise we will select any value
		p.acceptedAggregates[accepted.Sequence] = newAggregate(p.qsize)
		return []MessageTo{
			{Message: types.NewAcceptMessage(p.ballot.Get()+1, accepted.Sequence, value)},
		}, nil
	}
	return nil, nil
}

func (p *paxos) stepLearned(msg MessageFrom) ([]MessageTo, error) {
	learned := msg.Message.GetLearned()
	p.log.Commit(learned.Values...)
	return []MessageTo{
		{
			Message: types.NewUpdatePromiseMessage(p.ballot.Get(), p.log.Commited()),
			To:      []uint64{msg.From},
		},
	}, nil
}

func (p *paxos) updateReplicas() (messages []MessageTo, err error) {
	commited := p.log.Commited()
	for i := range p.replicas {
		if i == p.replicaID {
			continue
		}
		rcomm := p.commited.Get(i)
		// in paxos coordinator can be elected even if his log is not the most recent
		// in such case he will eventually catch up
		if rcomm >= commited {
			continue
		}
		values := p.log.List(rcomm, commited)
		messages = append(messages, MessageTo{Message: types.NewLearnedMessage(values...), To: []uint64{i}})
	}
	return messages, nil
}

// skipPrepareAllowed is allowed if coordinator has the most recent log among majority of the cluster.
func (p *paxos) skipPrepareAllowed() bool {
	commited := p.log.Commited()
	count := 0
	for i := range p.replicas {
		rcomm := p.commited.Get(i)
		if commited >= rcomm {
			count++
		}
		if count == p.qsize {
			return true
		}
	}
	return false
}
