package network

import (
	"context"

	"github.com/dshulyak/rapid/types"
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

func (bf BroadcastFacade) Egress() chan<- []*types.Message {
	return bf.rb.Egress()
}
