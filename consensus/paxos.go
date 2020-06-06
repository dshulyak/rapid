package consensus

import (
	"time"

	"go.uber.org/zap"

	"github.com/dshulyak/rapid/types"
)

type Config struct {
	Node          *types.Node
	Configuration *types.Configuration
	Period        time.Duration
	Timeout       int
}

func getFastQuorum(lth int) int {
	return (lth * 2 / 3) + 1
}

func getSafety(lth int) int {
	return (lth / 3) + 1
}

func getClassicQuorum(lth int) int {
	return (lth / 2) + 1
}

func NewPaxos(logger *zap.SugaredLogger, conf Config) *Paxos {
	logger = logger.Named("paxos").With("node", conf.Node.ID)
	n := len(conf.Configuration.Nodes)
	replicas := map[uint64]struct{}{}
	for _, node := range nodes {
		replicas[node.ID] = struct{}{}
	}
	classic := getClassicQuorum(n)
	fast := getFastQuorum(n)
	safety := getSafety(n)
	accepted := map[uint64]*aggregate{}
	accepted[0] = newAggregate(fast, safety)
	return &Paxos{
		conf:          conf,
		replicaID:     conf.Node.ID,
		replicas:      replicas,
		instanceID:    conf.Configuration.ID,
		mainLogger:    logger,
		logger:        logger,
		classicQuorum: classic,
		fastQuorum:    fast,
		safetyQuorum:  safety,
		promised:      newAggregate(classic, safety),
		accepted:      accepted,
		log:           newValues(),
	}
}

// Paxos is an implementation of fast multipaxos variant of the algorithm.
// it is not thread safe, and meant to be wrapped with component that we will serialize
// access to public APIs of Paxos.
type Paxos struct {
	conf Config

	replicaID uint64
	replicas  map[uint64]struct{}

	// instanceID and classic/fast quorums must be updated after each configuration commit.
	instanceID                              uint64
	classicQuorum, fastQuorum, safetyQuorum int

	// logger is extended with context information
	mainLogger, logger *zap.SugaredLogger

	// timeout ticks
	ticks int

	// promiseAggregate maps sequence to promise messages
	promised *aggregate
	// accepted tracks messages for every ballot
	accepted map[uint64]*aggregate

	// pendingAccept is not nil if node is ready to send Accept with proposed value
	// instead of queuing proposal.
	pendingAccept *types.Message

	// every replicas can propose once in the paxos instance
	proposed bool
	log      *values
	ballot   uint64

	// messages are consumed by paxos reactor and will be sent over the network
	Messages []*types.Message
	// values meant to be consumed by state machine application.
	Values []*types.LearnedValue
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
		p.promised = newAggregate(p.classicQuorum, p.safetyQuorum)
		p.logger.With(
			"ballot", p.ballot+1,
			"insntance", p.instanceID+1,
		).Debug("sending prepare")
		p.send(types.NewPrepareMessage(p.ballot+1, p.instanceID+1))
	}
}

func (p *Paxos) renewTimeout() {
	p.ticks = p.conf.Timeout
}

func (p *Paxos) Propose(value *types.Value) {
	if p.proposed {
		p.logger.With(
			"ballot", p.ballot,
		).Debug("already voted in this ballot")
		return
	}
	p.ticks = p.conf.Timeout
	p.proposed = true
	p.logger.With(
		"ballot", p.ballot,
		"instance", p.instanceID+1,
	).Debug("voted for a value")

	p.log.add(&types.LearnedValue{
		Ballot:   p.ballot,
		Sequence: p.instanceID + 1,
		Value:    value,
	})
	p.send(types.NewAcceptedMessage(p.ballot, p.instanceID+1, value))
}

// TODO we can commit only one value per paxos instance
func (p *Paxos) commit(v *types.LearnedValue) {
	p.Values = append(p.Values, v)
	p.log.commit(v)

	p.classicQuorum = getClassicQuorum(len(v.Value.Nodes))
	p.fastQuorum = getFastQuorum(len(v.Value.Nodes))
	p.accepted = map[uint64]*aggregate{
		0: newAggregate(p.fastQuorum, p.safetyQuorum),
	}
	p.promised = newAggregate(p.classicQuorum, p.safetyQuorum)
	p.proposed = false
	p.replicas = map[uint64]struct{}{}
	for _, node := range v.Value.Nodes {
		p.replicas[node.ID] = struct{}{}
	}
	p.instanceID = v.Sequence
	p.ballot = 0
	p.ticks = -1

	p.logger.With(
		"sequence", v.Sequence,
		"classic", p.classicQuorum,
		"fast", p.fastQuorum,
	).Info("updated configuration")
	// TODO if node with id was removed from cluster - panic and make application
	// bootstrap itself again
}

func (p *Paxos) send(msg *types.Message, to ...uint64) {
	// if replica is not part of the current cluster it should not try
	// to participate in consensus
	if _, exist := p.replicas[p.replicaID]; !exist {
		return
	}
	msg = types.WithRouting(p.replicaID, to, msg)
	msg = types.WithInstance(p.instanceID, msg)
	if to == nil {
		p.Messages = append(p.Messages, msg)
		p.Step(msg)
	}
	for i := range to {
		if to[i] != p.replicaID {
			continue
		}
		p.Step(msg)
		break
	}
}

func (p *Paxos) Step(msg *types.Message) {
	p.logger = p.mainLogger.With(
		"ballot", p.ballot,
		"instance", p.instanceID,
		"msg instance", msg.InstanceID,
		"from", msg.From,
	)
	defer func() {
		p.logger = p.mainLogger
	}()
	// TODO change to trace. requires custom level
	p.logger.With("message", msg).Debug("step")

	if msg.InstanceID < p.instanceID {
		p.logger.Debug("old message")
		return
	} else if msg.InstanceID > p.instanceID {
		p.logger.Debug("node is running with outdated configuration")
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
	p.send(
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
		value, _ := p.promised.safe()
		if value == nil {
			value = p.promised.any()
		}
		p.promised = newAggregate(p.classicQuorum, p.safetyQuorum)
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
	if accepted.Ballot > p.ballot {
		p.ballot = accepted.Ballot
	}
	if _, exist := p.accepted[accepted.Ballot]; !exist {
		p.accepted[accepted.Ballot] = newAggregate(p.classicQuorum, p.safetyQuorum)
	}
	p.logger.With(
		"from", msg.From,
		"sequence", accepted.Sequence,
	).Debug("aggregating accepted message")
	p.accepted[accepted.Ballot].add(msg.From, accepted.Ballot, accepted.Value)
	if p.accepted[accepted.Ballot].complete() {
		value, final := p.accepted[accepted.Ballot].safe()
		if final {
			p.commit(&types.LearnedValue{
				Ballot:   accepted.Ballot,
				Sequence: accepted.Sequence,
				Value:    value,
			})
		}
	}
}
