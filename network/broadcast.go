package network

import (
	"context"
	"time"

	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
)

type Network interface {
	Register(func(context.Context, []*types.BroadcastMessage) error)
	Dial(context.Context, *types.Node) (Connection, error)
}

type Server interface {
	Register(func(context.Context, []*types.BroadcastMessage) error)
}

type Connection interface {
	Send(context.Context, []*types.BroadcastMessage) error
	Close() error
}

type Config struct {
	NodeID      uint64
	QueueSize   int
	DialTimeout time.Duration
	SendTimeout time.Duration
	RetryPeriod time.Duration
}

func NewReliableBroadcast(logger *zap.SugaredLogger, network Network, last *graph.LastKG, conf Config) ReliableBroadcast {
	rb := ReliableBroadcast{
		conf:    conf,
		logger:  logger,
		network: network,
		last:    last,
		ingress: make(chan []*types.BroadcastMessage, 1),
		egress:  make(chan []*types.BroadcastMessage, 1),
		watch:   make(chan []*types.BroadcastMessage, 1),
	}
	network.Register(rb.receive)
	return rb
}

type ReliableBroadcast struct {
	conf Config

	logger  *zap.SugaredLogger
	network Network
	last    *graph.LastKG

	ingress, egress, watch chan []*types.BroadcastMessage
}

func (rb ReliableBroadcast) receive(ctx context.Context, msgs []*types.BroadcastMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case rb.ingress <- msgs:
		return nil
	}
}

func (rb ReliableBroadcast) Egress() chan<- []*types.BroadcastMessage {
	return rb.egress
}

func (rb ReliableBroadcast) Watch() <-chan []*types.BroadcastMessage {
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
	kg, update := rb.last.Last()
	state := &broadcastState{
		kg:          kg,
		update:      update,
		connections: map[uint64]connection{},
		network:     rb.network,
		conf:        rb.conf,
		logger:      rb.logger,
		ctx:         ctx,
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
		state.kg, state.update = rb.last.Last()
		state.reorg()
	case state.received = <-rb.ingress:
		state.watching = rb.watch
		for i := range state.connections {
			if err := state.connections[i].Send(state.ctx, state.received); err != nil {
				rb.logger.With(
					"error", err,
					"node", i,
				).Error("failed to send messages")
			}
		}
	case state.watching <- state.received:
		state.watching = nil
		state.received = nil
	case msgs := <-rb.egress:
		for i := range state.connections {
			if err := state.connections[i].Send(state.ctx, msgs); err != nil {
				rb.logger.With(
					"error", err,
					"node", i,
				).Error("failed to send messages")
			}
		}
	}
	return nil
}
