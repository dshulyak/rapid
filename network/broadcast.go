package network

import (
	"context"
	"time"

	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
)

type Network interface {
	RegisterBroadcasterServer(func(context.Context, []*types.BroadcastMessage) error)
	Dialer
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
	network.RegisterBroadcasterServer(rb.receive)
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
		state.kg, state.update = rb.last.Last()
		state.reorg()
	case state.watching <- state.received:
		state.watching = nil
		state.received = nil
	case received := <-rb.ingress:
		filtered := make([]*types.BroadcastMessage, 0, len(received))
		for _, msg := range received {
			if state.isNew(msg) {
				filtered = append(filtered, msg)
			}
		}
		if len(filtered) == 0 {
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
		}
		for i := range state.peers {
			_ = state.peers[i].Send(state.ctx, tosend)
		}
	}
	return nil
}
