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
	InstanceID                []byte
	Replicas                  []uint64
}

func NewPaxos(logger *zap.SugaredLogger, conf Config) *Paxos {
	replicas := map[uint64]*replicaState{}
	for _, r := range conf.Replicas {
		replicas[r] = &replicaState{}
	}
	logger = logger.Named("paxos").With("node", conf.ReplicaID)
	return &Paxos{
		conf:               conf,
		instanceID:         conf.InstanceID,
		mainLogger:         logger,
		logger:             logger,
		replicas:           replicas,
		promiseAggregates:  map[uint64]*aggregate{},
		acceptedAggregates: map[uint64]*aggregate{},
		log:                NewLog(),
		commited:           NewCommitedState(conf.Replicas),
		proposed:           newQueue(),
	}
}

var _ Backend = (*Paxos)(nil)

// Paxos is an implementation of fast multipaxos variant of the algorithm.
// it is not thread safe, and meant to be wrapped with component that we will serialize
// access to public APIs of Paxos.
type Paxos struct {
	conf Config

	// instanceID changes when new configuration is applied.
	// paxos instance should not accept messages from other instances, except for the Learned message
	// if they are with higher sequence number.
	instanceID []byte

	// logger is extended with context information
	mainLogger, logger *zap.SugaredLogger

	// elected is true if coordinator completed 1st phase of paxos succesfully.
	elected bool

	// we need to have information if replica is currently reachable or not
	// to avoid generating/sending useless messages
	// use Accept{Any} as a heartbeat message
	replicas map[uint64]*replicaState

	// ticks are for leader election timeout
	ticks int

	// promiseAggregate maps sequence to promise messages
	promiseAggregates  map[uint64]*aggregate
	acceptedAggregates map[uint64]*aggregate
	proposed           *queue

	// pendingAccept is not nil if node is ready to send Accept with proposed value
	// right when proposed value is delivered.
	pendingAccept *types.Message

	log    *Log
	ballot uint64
	// commited is not nil only for the coordinator
	commited *CommitedState

	messages []*types.Message

	// values meant to be consumed by state machine application.
	values []*types.LearnedValue
}

func (p *Paxos) Tick() {
	if !p.elected {
		p.ticks++
		// if node elected it needs to renounce leadership before starting election timeout
		if p.ticks >= p.conf.Timeout {
			p.ticks = 0
			p.slowBallot(p.ballot + 1)
		}
	} else {
		for id, state := range p.replicas {
			if id == p.conf.ReplicaID {
				continue
			}
			state.ticks++
			if state.ticks >= p.conf.HeartbeatTimeout {
				p.logger.Debug("sent hearbeat.", " to=", id)
				p.send(types.NewAcceptMessage(p.ballot, p.log.Commited(), nil), id)
				state.ticks = 0
			}
		}
	}
}

func (p *Paxos) Propose(value *types.Value) {
	if p.pendingAccept != nil {
		p.logger.Debug("accepted proposed value=", value)
		accept := p.pendingAccept.GetAccept()
		p.log.Add(&types.LearnedValue{
			Ballot:   accept.Ballot,
			Sequence: accept.Sequence,
			Value:    value,
		})
		p.send(types.NewAcceptedMessage(accept.Ballot, accept.Sequence, value), p.pendingAccept.From)
		p.pendingAccept = nil
	} else {
		p.logger.Debug("queued proposed value=", value)
		p.proposed.add(value)
	}
}

func (p *Paxos) slowBallot(bal uint64) {
	seq := p.log.Commited() + 1
	p.promiseAggregates[seq] = newAggregate(p.conf.FastQuorum)
	p.logger.Debug("started election ballot.",
		" ballot=", bal,
		" sequence=", seq,
	)
	p.send(types.NewPrepareMessage(bal, seq))
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
		p.logger.Info("commited value=", v)
		if v.Sequence > p.log.Commited() {
			p.values = append(p.values, v)
			p.log.Commit(v)
		}
	}
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
		out := types.WithRouting(p.conf.ReplicaID, to, &msg)
		out = types.WithInstance(p.instanceID, out)
		p.replicas[to].ticks = 0
		if msg.To == p.conf.ReplicaID {
			p.logger.Debug("sending to self=", out)
			p.Step(out)
			continue
		}
		p.logger.Debug("sending=", out)
		p.messages = append(p.messages, out)
	}
}

func (p *Paxos) Step(msg *types.Message) {
	p.logger = p.mainLogger.With("ballot", p.ballot)
	p.logger.Debug("processing=", msg.String())
	if msg.To != p.conf.ReplicaID {
		p.logger.Error("delivered to wrong node.", " message=", msg)
		return
	}

	if learned := msg.GetLearned(); learned != nil {
		p.stepLearned(msg)
		return
	}

	if bytes.Compare(msg.InstanceID, p.instanceID) != 0 {
		return
	}

	if prepare := msg.GetPrepare(); prepare != nil {
		p.stepPrepare(msg)
	}
	if promise := msg.GetPromise(); promise != nil {
		p.stepPromise(msg)
	}
	if accept := msg.GetAccept(); accept != nil {
		p.stepAccept(msg)
	}
	if accepted := msg.GetAccepted(); accepted != nil {
		p.stepAccepted(msg)
	}
}

// TODO move to Acceptor role.
func (p *Paxos) stepPrepare(msg *types.Message) {
	prepare := msg.GetPrepare()
	if prepare.Ballot <= p.ballot {
		// return only a ballot for the sender to update himself
		p.logger.Debug("prepare with old ballot.", " current=", p.ballot, " prepare=", prepare.Ballot)
		p.send(types.NewFailedPromiseMessage(p.ballot), msg.From)
		return
	}
	if prepare.Sequence < p.log.Commited() {
		// update peer with last known commited value
		p.send(types.NewLearnedMessage(p.log.Get(p.log.Commited())), msg.From)
		return
	}
	p.ticks = 0
	p.ballot = prepare.Ballot
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
			p.ballot,
			prepare.Sequence,
			voted,
			p.log.Commited(),
			value),
		msg.From,
	)
	return
}

// TODO stepPromise must be processed by Coordinator role.
func (p *Paxos) stepPromise(msg *types.Message) {
	promise := msg.GetPromise()
	if promise.Ballot > p.ballot {
		p.logger.Debug("received newer ballot in promise=", msg)
		p.ballot = promise.Ballot
		p.renounceLeadership()
		return
	}
	// ignore old messages
	if promise.Ballot < p.ballot {
		return
	}
	p.commited.Update(msg.From, promise.CommitedSequence)
	p.updateReplicas()

	agg, exist := p.promiseAggregates[promise.Sequence]
	if !exist {
		p.logger.Debug("ignored promise=", msg)
		return
	}

	agg.add(msg.From, promise.Ballot, promise.Value)
	if agg.complete() {
		p.logger.Info("node is elected")
		p.elected = true
		delete(p.promiseAggregates, promise.Sequence)
		p.acceptedAggregates[promise.Sequence] = newAggregate(p.conf.FastQuorum)

		value, _ := agg.safe()
		p.logger.Debug("waiting for accepted messages.",
			" ballot=", p.ballot,
			" sequence=", promise.Sequence,
			" is-any=", value == nil,
		)
		if value == nil {
			value = anyValue
		}
		p.send(types.NewAcceptMessage(p.ballot, promise.Sequence, value))
		return
	}
	return
}

// Accept message received by Acceptors
func (p *Paxos) stepAccept(msg *types.Message) {
	accept := msg.GetAccept()
	if p.ballot > accept.Ballot {
		// it can also return failed accepted message, but returning failed promise is
		// safe from protocol pov and tehcnically achieves the same result.
		// the purpose is the same as in stepPrepare - for coordinator to update himself
		p.send(types.NewFailedPromiseMessage(p.ballot), msg.From)
		return
	}
	if accept.Sequence < p.log.Commited() {
		// update peer with last known commited value
		p.send(types.NewLearnedMessage(p.log.Get(p.log.Commited())), msg.From)
		return
	}

	p.ballot = accept.Ballot
	p.ticks = 0
	value := accept.Value
	if value == nil {
		p.logger.Debug("received heartbeat.", " from=", msg.From)
		return
	}
	if IsAny(value) {
		learned := p.log.Get(accept.Sequence)
		if learned != nil {
			value = learned.Value
		} else if p.proposed.empty() {
			p.pendingAccept = msg
			return
		} else {
			value = p.proposed.pop()
		}
	}
	learned := &types.LearnedValue{
		Ballot:   accept.Ballot,
		Sequence: accept.Sequence,
		Value:    value,
	}
	p.logger.Debug("accepted value=", learned)
	p.log.Add(learned)
	p.send(types.NewAcceptedMessage(accept.Ballot, accept.Sequence, value), msg.From)
	p.pendingAccept = nil
	return
}

// Accepted received by Coordinator.
func (p *Paxos) stepAccepted(msg *types.Message) {
	accepted := msg.GetAccepted()
	if p.ballot > accepted.Ballot {
		p.ballot = accepted.Ballot
		p.renounceLeadership()
		return
	}
	// TODO send that node an update with new ballot
	if accepted.Ballot < p.ballot {
		return
	}
	agg, exist := p.acceptedAggregates[accepted.Sequence]
	if !exist {
		p.logger.Debug("ignored accepted.",
			" ballot=", accepted.Ballot,
			" sequence=", accepted.Sequence,
			" from=", msg.From)
		return
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
				p.slowBallot(p.ballot + 1)
			} else {
				bal := p.ballot
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
			return
		}
		// coordinated recovery
		if value == nil {
			value = agg.any()
		}
		p.acceptedAggregates[accepted.Sequence] = newAggregate(p.conf.FastQuorum)
		next := p.ballot + 1
		p.ballot = next
		p.send(types.NewAcceptMessage(next, accepted.Sequence, value))
		return
	}
	return
}

func (p *Paxos) stepLearned(msg *types.Message) {
	learned := msg.GetLearned()
	p.commit(learned.Values...)
	p.send(types.NewUpdatePromiseMessage(p.ballot, p.log.Commited()), msg.From)
	return
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
		p.send(types.NewLearnedMessage(p.log.Get(commited)), i)
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
