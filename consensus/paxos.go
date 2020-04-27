package consensus

import (
	"bytes"
	"errors"

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

func NewPaxos(logger *zap.SugaredLogger, store Persistence, conf Config) *Paxos {
	replicas := map[uint64]struct{}{}
	for _, r := range conf.Replicas {
		replicas[r] = struct{}{}
	}
	return &Paxos{
		logger:             logger,
		store:              store,
		replicaID:          conf.ReplicaID,
		qsize:              conf.Quorum,
		timeout:            conf.Timeout,
		replicas:           replicas,
		promiseAggregates:  map[uint64]*aggregate{},
		acceptedAggregates: map[uint64]*aggregate{},
		ballot:             NewBallot(store),
		log:                NewLog(store),
		commited:           NewCommitedState(store, conf.Replicas),
	}
}

// Paxos is an implementation of fast multipaxos variant of the algorithm.
type Paxos struct {
	logger *zap.SugaredLogger
	store  Persistence

	// configuration
	replicaID uint64
	qsize     int
	timeout   int

	// TODO add heartbeat timeout

	// TODO we need to have information if replica is currently reachable or not
	// to avoid generating/sending useless messages
	// TODO heartbeat ticks should stored per replica
	// use Accept{Any} as a heartbeat message
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

func (p *Paxos) Tick() ([]MessageTo, error) {
	p.ticks++
	if p.ticks == p.timeout {
		p.ticks = 0
		return p.slowBallot(p.ballot.Get() + 1)
	}
	return nil, nil
}

func (p *Paxos) Propose(value *types.Value) error {
	p.proposed.add(value)
	return nil
}

func (p *Paxos) slowBallot(bal uint64) ([]MessageTo, error) {
	seq := p.log.Commited() + 1
	p.promiseAggregates[seq] = newAggregate(p.qsize)
	p.ballot.Set(bal)
	return []MessageTo{{Message: types.NewPrepareMessage(bal, seq)}}, nil
}

func (p *Paxos) Step(msg MessageFrom) ([]MessageTo, error) {
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
func (p *Paxos) stepPrepare(msg MessageFrom) ([]MessageTo, error) {
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
func (p *Paxos) stepPromise(msg MessageFrom) ([]MessageTo, error) {
	promise := msg.Message.GetPromise()
	if promise.Ballot > p.ballot.Get() {
		p.logger.Debug("received newer ballot in promise.", " ballot=", promise.Ballot, " from=", msg.From)
		// TODO step down from leader role
		p.ballot.Set(promise.Ballot)
		return nil, nil
	}
	// ignore old messages
	if promise.Ballot < p.ballot.Get() {
		return nil, nil
	}
	p.commited.Update(msg.From, promise.CommitedSequence)
	msgs, err := p.updateReplicas()
	if err != nil {
		return nil, err
	}

	agg, exist := p.promiseAggregates[promise.Sequence]
	if !exist {
		p.logger.Debug("useless message.", " ballot=", promise.Ballot, " sequence=", promise.Sequence)
		return nil, nil
	}

	agg.add(msg.From, promise.Ballot, promise.Value)
	if agg.complete() {
		delete(p.promiseAggregates, promise.Sequence)
		p.acceptedAggregates[promise.Sequence] = newAggregate(p.qsize)

		value, _ := agg.safe()
		if value == nil {
			value = anyValue
		}
		return append(msgs, MessageTo{
			Message: types.NewAcceptMessage(p.ballot.Get(), promise.Sequence, value),
		}), nil
	}
	return nil, nil
}

// Accept message received by Acceptors
func (p *Paxos) stepAccept(msg MessageFrom) ([]MessageTo, error) {
	accept := msg.Message.GetAccept()
	if p.ballot.Get() > accept.Ballot {
		// it can also return failed accepted message, but returning failed promise is
		// safe from protocol pov and tehcnically achieves the same result.
		// the purpose is the same as in stepPrepare - for coordinator to update himself
		return []MessageTo{
			{
				Message: types.NewFailedPromiseMessage(p.ballot.Get()),
				To:      []uint64{msg.From},
			},
		}, nil
	}
	p.ticks = 0
	value := accept.Value
	if value == nil {
		p.logger.Debug("received heartbeat.", " from=", msg.From)
		return nil, nil
	}
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

// Accepted received by Coordinator.
func (p *Paxos) stepAccepted(msg MessageFrom) ([]MessageTo, error) {
	accepted := msg.Message.GetAccepted()
	if p.ballot.Get() > accepted.Ballot {
		// TODO step down from leader role
		p.ballot.Set(accepted.Ballot)
		return nil, nil
	}
	// ignore old messages
	if accepted.Ballot < p.ballot.Get() {
		return nil, nil
	}
	agg, exist := p.acceptedAggregates[accepted.Sequence]
	if !exist {
		p.logger.Debug("useless message.", " ballot=", accepted.Ballot, " sequence=", accepted.Sequence)
		return nil, nil
	}
	agg.add(msg.From, accepted.Ballot, accepted.Value)
	if agg.complete() {
		value, final := agg.safe()
		if final {
			delete(p.acceptedAggregates, accepted.Sequence)
			learned := &types.LearnedValue{
				Ballot:   accepted.Ballot,
				Sequence: accepted.Sequence,
				Value:    value,
			}
			p.log.Commit(learned)

			var msgs []MessageTo
			if !p.skipPrepareAllowed() {
				start, err := p.slowBallot(p.ballot.Get() + 1)
				if err != nil {
					return nil, err
				}
				msgs = append(msgs, start...)
			} else {
				msgs = append(msgs, MessageTo{
					Message: types.NewAcceptMessage(p.ballot.Get(), p.log.Commited()+1, anyValue),
				})
				p.acceptedAggregates[p.log.Commited()+1] = newAggregate(p.qsize)
			}

			updates, err := p.updateReplicas()
			if err != nil {
				return nil, err
			}

			return append(msgs, updates...), nil
		} else {
			// coordinated recovery
			if value == nil {
				value = agg.any()
			}
			p.acceptedAggregates[accepted.Sequence] = newAggregate(p.qsize)
			next := p.ballot.Get() + 1
			p.ballot.Set(next)
			return []MessageTo{
				{Message: types.NewAcceptMessage(next, accepted.Sequence, value)},
			}, nil
		}
	}
	return nil, nil
}

func (p *Paxos) stepLearned(msg MessageFrom) ([]MessageTo, error) {
	learned := msg.Message.GetLearned()
	p.log.Commit(learned.Values...)
	return []MessageTo{
		{
			Message: types.NewUpdatePromiseMessage(p.ballot.Get(), p.log.Commited()),
			To:      []uint64{msg.From},
		},
	}, nil
}

func (p *Paxos) updateReplicas() (messages []MessageTo, err error) {
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
func (p *Paxos) skipPrepareAllowed() bool {
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
