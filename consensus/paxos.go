package consensus

import (
	"encoding/hex"
	"math"

	"github.com/dshulyak/rapid/consensus/types"
	"go.uber.org/zap"

	atypes "github.com/dshulyak/rapid/types"
)

type Config struct {
	Node          *atypes.Node
	Configuration *atypes.Configuration

	Timeout int
}

func getFastQuorum(lth int) int {
	return int(math.Ceil(float64(lth) * 3 / 4))
}

func getClassicQuorum(lth int) int {
	return (lth / 2) + 1
}

func NewPaxos(logger *zap.SugaredLogger, conf Config) *Paxos {
	logger = logger.Named("paxos").With("node", conf.Node.ID)
	classic := getClassicQuorum(len(conf.Configuration.Nodes))
	fast := getFastQuorum(len(conf.Configuration.Nodes))
	return &Paxos{
		conf:          conf,
		replicaID:     conf.Node.ID,
		instanceID:    conf.Configuration.ID,
		mainLogger:    logger,
		logger:        logger,
		replicas:      newReplicasInfo(conf.Configuration.Nodes),
		classicQuorum: classic,
		fastQuorum:    fast,
		promised:      newAggregate(classic, fast/2),
		accepted:      newAggregate(fast, fast/2),
		log:           newValues(),
	}
}

var _ Backend = (*Paxos)(nil)

// Paxos is an implementation of fast multipaxos variant of the algorithm.
// it is not thread safe, and meant to be wrapped with component that we will serialize
// access to public APIs of Paxos.
type Paxos struct {
	conf Config

	replicaID uint64

	// instanceID and classic/fast quorums must be updated after each configuration commit.
	instanceID                uint64
	classicQuorum, fastQuorum int

	// logger is extended with context information
	mainLogger, logger *zap.SugaredLogger

	// we need to have information if replica is currently reachable or not
	// to avoid sending useless messages
	// use Accept{Any} as a heartbeat message
	replicas replicasInfo

	// timeout ticks
	ticks int

	// promiseAggregate maps sequence to promise messages
	promised *aggregate
	accepted *aggregate

	// pendingAccept is not nil if node is ready to send Accept with proposed value
	// instead of queuing proposal.
	pendingAccept *types.Message

	// every replicas can propose once in the paxos instance
	proposed bool
	log      *values
	ballot   uint64

	// messages are consumed by paxos reactor and will be sent over the network
	messages []*types.Message
	// values meant to be consumed by state machine application.
	values []*types.LearnedValue
}

// instantiate ticks once value was proposed
// if after ticks value wasn't commited - start a classic round
func (p *Paxos) Tick() {
	if p.ticks == -1 {
		// timeout is disabled
		return
	}
	p.ticks--
	if p.ticks == 0 {
		p.renewTimeout()
		// start classic round
		p.promised = newAggregate(p.classicQuorum, p.fastQuorum/2)
		p.send(types.NewPrepareMessage(p.ballot+1, p.instanceID+1))
	}
}

func (p *Paxos) renewTimeout() {
	p.ticks = p.conf.Timeout
}

func (p *Paxos) Propose(value *types.Value) {
	if !p.proposed {
		p.logger.With(
			"ballot", p.ballot,
		).Debug("already voted in this ballot")
	}
	p.ticks = p.conf.Timeout
	p.proposed = true
	p.logger.With(
		"value ID", hex.EncodeToString(value.Id),
		"ballot", p.ballot,
	).Debug("voted for a value")

	p.log.add(&types.LearnedValue{
		Ballot:   p.ballot,
		Sequence: p.instanceID + 1,
		Value:    value,
	})
	p.send(types.NewAcceptedMessage(p.ballot, p.instanceID+1, value))

}

// Messages returns staged messages and clears ingress of messages
func (p *Paxos) Messages() []*types.Message {
	msgs := p.messages
	p.messages = nil
	return msgs
}

// TODO we can commit only one value per paxos instance
func (p *Paxos) commit(values ...*types.LearnedValue) {
	for _, v := range values {
		if v.Sequence > p.log.commited() {
			p.values = append(p.values, v)
			p.log.commit(v)
			p.instanceID = v.Sequence
			p.replicas.update(v.Value.Changes)
			p.classicQuorum = getClassicQuorum(len(v.Value.Nodes))
			p.fastQuorum = getFastQuorum(len(v.Value.Nodes))
			p.accepted = newAggregate(p.fastQuorum, p.fastQuorum/2)
			p.promised = nil
			p.proposed = false
			p.logger.With(
				"sequence", v.Sequence,
				"classic", p.classicQuorum,
				"fast", p.fastQuorum,
			).Info("updating configuration")
			// TODO if node with id was removed from cluster - panic and make application
			// bootstrap itself again
		}
	}
}

func (p *Paxos) Values() []*types.LearnedValue {
	values := p.values
	p.values = nil
	return values
}

func (p *Paxos) send(original *types.Message) {
	p.replicas.iterate(func(state *replicaState) bool {
		msg := *original
		p.sendOne(&msg, state.id)
		return true
	})
}

func (p *Paxos) sendOne(msg *types.Message, to uint64) {
	msg = types.WithRouting(p.replicaID, to, msg)
	msg = types.WithInstance(p.instanceID, msg)
	p.replicas.resetTicks(to)
	if msg.To == p.replicaID {
		p.logger.With("msg", msg).Debug("sending to self")
		p.Step(msg)
		return
	}
	p.logger.With("msg", msg).Debug("sending to network")
	p.messages = append(p.messages, msg)
}

func (p *Paxos) Step(msg *types.Message) {
	p.logger = p.mainLogger.With(
		"ballot", p.ballot,
		"instance", p.instanceID,
		"from", msg.From,
	)
	defer func() {
		p.logger = p.mainLogger
	}()
	p.logger.Debug("step")
	if msg.To != p.replicaID {
		p.logger.Error("delivered to wrong node.")
		return
	}

	if !p.replicas.exist(msg.From) {
		return
	}

	if msg.InstanceID != p.instanceID {
		p.logger.With(
			"msg instance", msg.InstanceID,
		).Debug("instance conflict")
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

func (p *Paxos) stepPrepare(msg *types.Message) {
	prepare := msg.GetPrepare()
	if prepare.Ballot <= p.ballot {
		// return only a ballot for the sender to update himself
		p.logger.Debug("old ballot")
		return
	}
	p.renewTimeout()
	p.ballot = prepare.Ballot
	learned := p.log.get(prepare.Sequence)
	var (
		value *types.Value
		voted uint64
	)
	if learned != nil {
		value = learned.Value
		voted = learned.Ballot
	}
	p.accepted = newAggregate(p.classicQuorum, p.fastQuorum/2)
	p.sendOne(
		types.NewPromiseMessage(
			p.ballot,
			prepare.Sequence,
			voted,
			value),
		msg.From,
	)
	return
}

func (p *Paxos) stepPromise(msg *types.Message) {
	promise := msg.GetPromise()
	if promise.Ballot > p.ballot {
		p.logger.Debug("received newer ballot in promise.")
		p.ballot = promise.Ballot
		return
	} else if promise.Ballot < p.ballot {
		return
	}

	p.promised.add(msg.From, promise.Ballot, promise.Value)
	if p.promised.complete() {
		p.accepted = newAggregate(p.classicQuorum, p.fastQuorum/2)
		value, _ := p.promised.safe()
		if value == nil {
			value = p.promised.any()
		}
		p.promised = newAggregate(p.classicQuorum, p.fastQuorum/2)
		p.send(types.NewAcceptMessage(p.ballot, promise.Sequence, value))
	}
}

// Accept message received by Acceptors
func (p *Paxos) stepAccept(msg *types.Message) {
	accept := msg.GetAccept()
	if p.ballot > accept.Ballot {
		return
	}

	p.renewTimeout()
	p.ballot = accept.Ballot
	value := accept.Value
	learned := &types.LearnedValue{
		Ballot:   accept.Ballot,
		Sequence: accept.Sequence,
		Value:    value,
	}
	p.logger.With("value", value).Debug("accepted")
	p.log.add(learned)
	// accepted must be broadcasted even in classic round
	p.send(types.NewAcceptedMessage(accept.Ballot, accept.Sequence, value))
}

func (p *Paxos) stepAccepted(msg *types.Message) {
	accepted := msg.GetAccepted()
	if p.ballot > accepted.Ballot {
		return
	} else if accepted.Ballot > p.ballot {
		p.renewTimeout()
		p.ballot = accepted.Ballot
		return
	}
	p.logger.With(
		"from", msg.From,
		"sequence", accepted.Sequence,
	).Debug("aggregating accepted message")
	p.accepted.add(msg.From, accepted.Ballot, accepted.Value)
	if p.accepted.complete() {
		value, final := p.accepted.safe()
		if final {
			p.commit(&types.LearnedValue{
				Ballot:   accepted.Ballot,
				Sequence: accepted.Sequence,
				Value:    value,
			})
		}
	}
}
