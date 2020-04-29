package consensus

import (
	"bytes"

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

type Config struct {
	Timeout                   int
	HeartbeatTimeout          int
	ReplicaID                 uint64
	ClassicQuorum, FastQuorum int
	Replicas                  []uint64
}

func NewPaxos(logger *zap.SugaredLogger, store Persistence, conf Config) *Paxos {
	replicas := map[uint64]*replicaState{}
	for _, r := range conf.Replicas {
		replicas[r] = &replicaState{}
	}
	return &Paxos{
		conf:               conf,
		logger:             logger,
		store:              store,
		replicas:           replicas,
		promiseAggregates:  map[uint64]*aggregate{},
		acceptedAggregates: map[uint64]*aggregate{},
		ballot:             NewBallot(store),
		log:                NewLog(store),
		commited:           NewCommitedState(store, conf.Replicas),
	}
}

var _ Backend = (*Paxos)(nil)

// Paxos is an implementation of fast multipaxos variant of the algorithm.
// it is not thread safe, and meant to be wrapped with component that we will serialize
// access to public APIs of Paxos.
type Paxos struct {
	conf Config

	logger *zap.SugaredLogger
	store  Persistence

	elected bool

	// we need to have information if replica is currently reachable or not
	// to avoid generating/sending useless messages
	// use Accept{Any} as a heartbeat message
	replicas map[uint64]*replicaState

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

	messages []*types.Message

	// values meant to be consumed by state machine application.
	values []*types.LearnedValue
}

func (p *Paxos) Tick() {
	p.ticks++
	// if node elected it needs to renounce leadership before starting election timeout
	if p.ticks >= p.conf.Timeout && !p.elected {
		p.ticks = 0
		p.slowBallot(p.ballot.Get() + 1)
	}
	if p.elected {
		for id, state := range p.replicas {
			if id == p.conf.ReplicaID {
				continue
			}
			state.ticks++
			if state.ticks >= p.conf.HeartbeatTimeout {
				p.logger.Debug("sent hearbeat.", " to=", id)
				p.send(types.NewAcceptMessage(p.ballot.Get(), p.log.Commited(), nil), id)
				state.ticks = 0
			}
		}
	}
}

func (p *Paxos) Propose(value *types.Value) error {
	// TODO if replica received Accept(Any) for the current ballot it should
	// send Accepted to coordinator as soon as value is added to submitssion queue.
	p.proposed.add(value)
	return nil
}

func (p *Paxos) slowBallot(bal uint64) {
	seq := p.log.Commited() + 1
	p.renounceLeadership()
	p.promiseAggregates[seq] = newAggregate(p.conf.FastQuorum)
	p.ballot.Set(bal)
	p.send(types.NewPrepareMessage(bal, seq))
	p.logger.Debug("started election ballot.",
		" ballot=", bal,
		" sequence=", seq,
	)
}

// Messages returns staged messages and clears ingress of messages
func (p *Paxos) Messages() []*types.Message {
	msgs := p.messages
	p.messages = nil
	return msgs
}

func (p *Paxos) renounceLeadership() {
	p.elected = false
}

// TODO commit must append values to p.values if they are commited in order.
func (p *Paxos) commit(values ...*types.LearnedValue) {
	for _, v := range values {
		// TODO insert several symbols from id as hex
		p.logger.Info("commited value",
			" ballot=", v.Ballot,
			" sequence=", v.Sequence,
		)
	}
	p.log.Commit(values...)
	p.values = append(p.values, values...)
}

func (p *Paxos) Values() []*types.LearnedValue {
	values := p.values
	p.values = nil
	return values
}

func (p *Paxos) send(original *types.Message, recipients ...uint64) {
	if len(recipients) == 0 {
		recipients = p.conf.Replicas
	}
	for _, to := range recipients {
		msg := *original
		msg.From = p.conf.ReplicaID
		msg.To = to
		p.replicas[to].ticks = 0
		p.messages = append(p.messages, &msg)
	}
}

func (p *Paxos) Step(msg *types.Message) {
	if prepare := msg.GetPrepare(); prepare != nil {
		_ = p.stepPrepare(msg)
	}
	if promise := msg.GetPromise(); promise != nil {
		_ = p.stepPromise(msg)
	}
	if accept := msg.GetAccept(); accept != nil {
		_ = p.stepAccept(msg)
	}
	if accepted := msg.GetAccepted(); accepted != nil {
		_ = p.stepAccepted(msg)
	}
	if learned := msg.GetLearned(); learned != nil {
		_ = p.stepLearned(msg)
	}
}

// TODO move to Acceptor role.
func (p *Paxos) stepPrepare(msg *types.Message) error {
	prepare := msg.GetPrepare()
	if prepare.Ballot <= p.ballot.Get() {
		// return only a ballot for the sender to update himself
		p.send(types.NewFailedPromiseMessage(p.ballot.Get()), msg.From)
		return nil
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
	p.send(
		types.NewPromiseMessage(
			p.ballot.Get(),
			prepare.Sequence,
			voted,
			p.log.Commited(),
			value),
		msg.From,
	)
	return nil
}

// TODO stepPromise must be processed by Coordinator role.
func (p *Paxos) stepPromise(msg *types.Message) error {
	promise := msg.GetPromise()
	if promise.Ballot > p.ballot.Get() {
		p.logger.Debug("received newer ballot in promise.", " ballot=", promise.Ballot, " from=", msg.From)
		p.ballot.Set(promise.Ballot)
		p.renounceLeadership()
		return nil
	}
	// ignore old messages
	if promise.Ballot < p.ballot.Get() {
		return nil
	}
	p.commited.Update(msg.From, promise.CommitedSequence)
	p.updateReplicas()

	agg, exist := p.promiseAggregates[promise.Sequence]
	if !exist {
		p.logger.Debug("ignored promise.", " ballot=", promise.Ballot, " sequence=", promise.Sequence)
		return nil
	}

	agg.add(msg.From, promise.Ballot, promise.Value)
	if agg.complete() {
		p.elected = true
		delete(p.promiseAggregates, promise.Sequence)
		p.acceptedAggregates[promise.Sequence] = newAggregate(p.conf.FastQuorum)

		value, _ := agg.safe()
		p.logger.Debug("waiting for accepted messages.",
			" ballot=", p.ballot.Get(),
			" sequence=", promise.Sequence,
			" is-any=", value == nil,
		)
		if value == nil {
			value = anyValue
		}
		p.send(types.NewAcceptMessage(p.ballot.Get(), promise.Sequence, value))
		return nil
	}
	return nil
}

// Accept message received by Acceptors
func (p *Paxos) stepAccept(msg *types.Message) error {
	accept := msg.GetAccept()
	if p.ballot.Get() > accept.Ballot {
		// it can also return failed accepted message, but returning failed promise is
		// safe from protocol pov and tehcnically achieves the same result.
		// the purpose is the same as in stepPrepare - for coordinator to update himself
		p.send(types.NewFailedPromiseMessage(p.ballot.Get()), msg.From)
		return nil
	}
	p.ballot.Set(accept.Ballot)
	p.ticks = 0
	value := accept.Value
	if value == nil {
		p.logger.Debug("received heartbeat.", " from=", msg.From)
		return nil
	}
	if IsAny(value) {
		learned := p.log.Get(accept.Sequence)
		if learned != nil {
			value = learned.Value
		} else if p.proposed.empty() {
			// make acceptor ready to sent accepted message
			// as soon as new item is published to proposals queue
			// otherwise deadlock is possible
			return nil
		} else {
			value = p.proposed.pop()
		}
	}
	p.log.Add(&types.LearnedValue{
		Ballot:   accept.Ballot,
		Sequence: accept.Sequence,
		Value:    value,
	})
	p.send(types.NewAcceptedMessage(accept.Ballot, accept.Sequence, value), msg.From)
	return nil
}

// Accepted received by Coordinator.
func (p *Paxos) stepAccepted(msg *types.Message) error {
	accepted := msg.GetAccepted()
	if p.ballot.Get() > accepted.Ballot {
		p.ballot.Set(accepted.Ballot)
		p.renounceLeadership()
		return nil
	}
	// TODO send that node an update with new ballot
	if accepted.Ballot < p.ballot.Get() {
		return nil
	}
	agg, exist := p.acceptedAggregates[accepted.Sequence]
	if !exist {
		p.logger.Debug("ignored accepted.",
			" ballot=", accepted.Ballot,
			" sequence=", accepted.Sequence,
			" from=", msg.From)
		return nil
	}
	agg.add(msg.From, accepted.Ballot, accepted.Value)
	if agg.complete() {
		value, final := agg.safe()
		if final {
			delete(p.acceptedAggregates, accepted.Sequence)
			p.commit(&types.LearnedValue{
				Ballot:   accepted.Ballot,
				Sequence: accepted.Sequence,
				Value:    value,
			})
			// start next ballot
			// we can skip Prepare phase if coordinator log is the most recent
			if !p.skipPrepareAllowed() {
				p.slowBallot(p.ballot.Get() + 1)
			} else {
				bal := p.ballot.Get()
				seq := p.log.Commited() + 1
				p.logger.Debug("waiting for accepted messages.",
					" ballot=", bal,
					" sequence=", seq,
					" is-any=", true,
				)
				p.send(types.NewAcceptMessage(bal, seq, anyValue))
				p.acceptedAggregates[seq] = newAggregate(p.conf.FastQuorum)
			}
			p.updateReplicas()
			return nil
		}
		// coordinated recovery
		if value == nil {
			value = agg.any()
		}
		p.acceptedAggregates[accepted.Sequence] = newAggregate(p.conf.FastQuorum)
		next := p.ballot.Get() + 1
		p.ballot.Set(next)
		p.send(types.NewAcceptMessage(next, accepted.Sequence, value))
		return nil
	}
	return nil
}

func (p *Paxos) stepLearned(msg *types.Message) error {
	learned := msg.GetLearned()
	p.commit(learned.Values...)
	p.send(types.NewUpdatePromiseMessage(p.ballot.Get(), p.log.Commited()))
	return nil
}

func (p *Paxos) updateReplicas() {
	commited := p.log.Commited()
	for i := range p.replicas {
		if i == p.conf.ReplicaID {
			continue
		}
		replicaCommited := p.commited.Get(i)
		// in paxos coordinator can be elected even if his log is not the most recent
		// in such case he will eventually catch up
		if replicaCommited >= commited {
			continue
		}
		values := p.log.List(replicaCommited, commited)
		p.send(types.NewLearnedMessage(values...), i)
	}
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
		if count == p.conf.FastQuorum {
			return true
		}
	}
	return false
}
