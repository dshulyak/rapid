package network

import (
	"context"
	"time"

	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
)

type Network interface {
	RegisterBroadcasterServer(func(context.Context, []*types.Message) error)
	Dialer
}

type Dialer interface {
	BroadcasterClient(context.Context, *types.Node) (Connection, error)
}

type Connection interface {
	Send(context.Context, []*types.Message) error
	Close() error
}

type Config struct {
	NodeID      uint64
	QueueSize   int
	Fanout      int
	DialTimeout time.Duration
	SendTimeout time.Duration
	RetryPeriod time.Duration
}

func NewReliableBroadcast(logger *zap.SugaredLogger,
	network Network,
	configuration *types.LastConfiguration,
	conf Config) ReliableBroadcast {
	rb := ReliableBroadcast{
		conf:          conf,
		logger:        logger.Named("broadcast"),
		network:       network,
		configuration: configuration,
		ingress:       make(chan []*types.Message, 1),
		egress:        make(chan []*types.Message, 1),
		watch:         make(chan []*types.Message, 1),
	}
	network.RegisterBroadcasterServer(rb.receive)
	return rb
}

// ReliableBroadcast guarantees that any sequnece of messages will
// be delivered atleast once and in order.
// Until peer is suspected by failure detector broadcaster will buffer
// and transmit message periodically.
// Once an ack received and message are in peer memory it is safe to stop.
// If peer crashes - it is peer responsbility to get missing data.
type ReliableBroadcast struct {
	conf Config

	logger        *zap.SugaredLogger
	network       Network
	configuration *types.LastConfiguration

	ingress, egress, watch chan []*types.Message
}

func (rb ReliableBroadcast) receive(ctx context.Context, msgs []*types.Message) error {
	select {
	case <-ctx.Done():
		rb.logger.Debug("request timedout")
		return ctx.Err()
	case rb.ingress <- msgs:
		return nil
	}
}

func (rb ReliableBroadcast) Egress() chan<- []*types.Message {
	return rb.egress
}

func (rb ReliableBroadcast) Watch() <-chan []*types.Message {
	return rb.watch
}

func (rb ReliableBroadcast) Run(ctx context.Context) error {
	state := rb.newState(ctx)
	for {
		if err := rb.selectOne(state); err != nil {
			return err
		}
	}
}

func (rb ReliableBroadcast) newState(ctx context.Context) *broadcastState {
	configuration, update := rb.configuration.Last()
	kg := graph.New(rb.conf.Fanout, configuration.Nodes)
	state := &broadcastState{
		kg:      kg,
		update:  update,
		peers:   map[uint64]peer{},
		network: rb.network,
		conf:    rb.conf,
		logger:  rb.logger,
		ctx:     ctx,
		seen:    map[uint64]uint64{},
	}
	state.reorg()
	return state
}

func (rb ReliableBroadcast) selectOne(state *broadcastState) error {
	select {
	case <-state.ctx.Done():
		state.wg.Wait()
		return state.ctx.Err()
	case <-state.update:
		configuration, update := rb.configuration.Last()
		kg := graph.New(rb.conf.Fanout, configuration.Nodes)
		state.kg, state.update = kg, update
		state.reorg()
	case state.watching <- state.received:
		state.watching = nil
		state.received = nil
	case received := <-rb.ingress:
		filtered := []*types.Message{}
		for _, msg := range received {
			nw := state.isNew(msg)
			if nw {
				filtered = append(filtered, msg)
			}
			rb.logger.With(
				"instance", msg.InstanceID,
				"type", msg.Type,
				"new", nw,
				"from", msg.From,
				"msg sequence", msg.SeqNum,
				"total", len(received),
			).Debug("received broadcast")
		}
		if len(filtered) == 0 {
			rb.logger.Debug("no new messages")
			return nil
		}

		state.received = append(state.received, filtered...)
		state.watching = rb.watch
		for i := range state.peers {
			_ = state.peers[i].Send(state.ctx, filtered)
		}
	case tosend := <-rb.egress:
		for i := range tosend {
			tosend[i].SeqNum = state.nextSeqNum()
			rb.logger.With(
				"instance", tosend[i].InstanceID,
				"type", tosend[i].Type,
			).Debugf("broadcasting")
		}
		for i := range state.peers {
			_ = state.peers[i].Send(state.ctx, tosend)
		}
	}
	return nil
}
