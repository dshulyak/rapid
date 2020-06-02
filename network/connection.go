package network

import (
	"context"
	"time"

	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

type connection struct {
	Stop func()

	node   *types.Node
	logger *zap.SugaredLogger

	net Network

	btimeout, dtimeout time.Duration
	retry              time.Duration

	bufsize int
	egress  chan []*types.BroadcastMessage
}

func (c connection) Send(ctx context.Context, msgs []*types.BroadcastMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.egress <- msgs:
		return nil
	}
}

func (c connection) Run(ctx context.Context) error {
	var (
		err      error
		requests = make(chan sendRequest, 1)
		errchan  = make(chan error, 1)
		response chan error

		// TODO pending shouldn't grow beyond bufsize
		// drop older messages in case of overflow
		pending []*types.BroadcastMessage
		added   []*types.BroadcastMessage

		retry = time.NewTimer(c.retry)
	)
	defer retry.Stop()
	go func() {
		errchan <- c.sendLoop(ctx, requests)
		close(errchan)
	}()
	for {
		select {
		case <-ctx.Done():
			close(requests)
			// last error will be an error from conn.Close call
			for err = range errchan {
			}
			if err != nil {
				return err
			}
			return ctx.Err()
		case err := <-response:
			if err != nil {
				retry.Reset(c.retry)
			} else if added != nil {
				pending, added = added, nil
				requests <- sendRequest{pending, errchan}
			} else {
				response = nil
			}
		case <-retry.C:
			if added != nil {
				pending, added = append(pending, added...), nil
			}
			requests <- sendRequest{pending, errchan}
		case msgs := <-c.egress:
			if response == nil {
				pending = append(pending, msgs...)
				requests <- sendRequest{pending, errchan}
				response = errchan
			} else {
				added = append(added, msgs...)
			}
		}
	}
}

func (c connection) sendLoop(ctx context.Context, requests chan sendRequest) error {
	var (
		conn Connection
		err  error
	)
	for req := range requests {
		if conn == nil {
			conn, err = c.net.Dial(ctx, c.node)
			if err != nil {
				// just in case
				conn = nil
				c.logger.With("error", err).Error("dial failed")
				req.error <- err
				continue
			}
		}
		err = conn.Send(ctx, req.msgs)
		if err != nil {
			c.logger.With("error", err).Error("send failed")
			req.error <- err
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

type sendRequest struct {
	msgs  []*types.BroadcastMessage
	error chan<- error
}
