package network

import (
	"context"
	"time"

	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

type Dialer interface {
	BroadcasterClient(context.Context, *types.Node) (Connection, error)
}

type peer struct {
	Stop func()

	node   *types.Node
	logger *zap.SugaredLogger

	net Dialer

	btimeout, dtimeout time.Duration
	retry              time.Duration

	bufsize int
	egress  chan []*types.BroadcastMessage
}

func (c peer) Send(ctx context.Context, msgs []*types.BroadcastMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.egress <- msgs:
		return nil
	}
}

func (c peer) Run(ctx context.Context) error {
	retry := time.NewTimer(c.retry)
	if !retry.Stop() {
		<-retry.C
	}
	defer retry.Stop()

	state := c.newPeerState(ctx)
	for {
		cont, err := c.selectOne(state, ctx, retry.C)
		if !cont {
			return err
		}
		if err != nil {
			retry.Reset(c.retry)
		}
	}
}

func (c peer) newPeerState(ctx context.Context) *peerState {
	state := newPeerState()
	go func() {
		state.Closech <- c.sendLoop(ctx, state.Inflight)
		close(state.Closech)
	}()
	return state
}

func (c peer) selectOne(state *peerState, ctx context.Context, retry <-chan time.Time) (bool, error) {
	select {
	case <-ctx.Done():
		close(state.Inflight)
		for err := range state.Closech {
			if err != nil {
				return false, err
			}
		}
		return false, ctx.Err()
	case state.Requests <- sendRequest{state.Added, state.Errchan}:
		// save pending set into a separate list, so that it can be discarded
		// after succesful execution
		state.Pending, state.Added = state.Added, nil
		// disable so that we don't send nil state.Added
		state.Requests = nil
	case err := <-state.Errchan:
		state.Err = err
		if err != nil {
			return true, err
		}
		// discard records that were delivered
		state.Pending = nil
	case <-retry:
		// in case of retry append added to pending to preserve original order
		// and renable requests
		state.Added, state.Pending = append(state.Pending, state.Added...), nil
		state.Requests = state.Inflight
	case msgs := <-c.egress:
		// queue messages and if there were no errors enable sending requests
		state.Added = append(state.Added, msgs...)
		if state.Err == nil {
			state.Requests = state.Inflight
		}
	}
	return true, nil

}

func (c peer) sendLoop(ctx context.Context, requests chan sendRequest) error {
	var (
		conn Connection
		err  error
	)
	for req := range requests {
		if conn == nil {
			conn, err = c.net.BroadcasterClient(ctx, c.node)
			if err != nil {
				conn = nil // just in case
				c.logger.With("error", err).Error("dial failed")
				req.error <- err
				continue
			}
		}
		err = conn.Send(ctx, req.msgs)
		if err != nil {
			c.logger.With("error", err).Error("send failed")
			req.error <- err
			continue
		}
		req.error <- nil
	}
	if conn != nil {
		if err := conn.Close(); err != nil {
			c.logger.With("error", err).Error("failed to close connection")
			return err
		}
	}
	return nil
}

func newPeerState() *peerState {
	return &peerState{
		Inflight: make(chan sendRequest),

		Errchan: make(chan error, 1),
		Closech: make(chan error, 1),
	}
}

type peerState struct {
	Requests chan sendRequest
	Inflight chan sendRequest

	Errchan chan error
	Closech chan error

	Pending []*types.BroadcastMessage
	Added   []*types.BroadcastMessage

	Err error
}

type sendRequest struct {
	msgs  []*types.BroadcastMessage
	error chan<- error
}
