package network

import (
	"context"

	"github.com/dshulyak/rapid/types"
	"golang.org/x/sync/errgroup"
)

func NewBroadcastFacade(rb ReliableBroadcast) BroadcastFacade {
	return BroadcastFacade{
		rb: rb,
		mx: NewMultiplexer(rb),
	}
}

type BroadcastFacade struct {
	rb ReliableBroadcast
	mx Multiplexer
}

func (bf BroadcastFacade) Subscribe(ctx context.Context) (*Subscription, error) {
	return bf.mx.Subscribe(ctx)
}

func (bf BroadcastFacade) Send(ctx context.Context, msgs []*types.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case bf.rb.Egress() <- msgs:
	}
	return nil
}

// Egress is for messages that are sent by this node into the network.
func (bf BroadcastFacade) Egress() chan<- []*types.Message {
	return bf.rb.Egress()
}

func (bf BroadcastFacade) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return bf.rb.Run(ctx)
	})
	group.Go(func() error {
		return bf.mx.Run(ctx)
	})
	return group.Wait()
}
