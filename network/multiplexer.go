package network

import (
	"context"

	"github.com/dshulyak/rapid/types"
)

type Subscription struct {
	context  context.Context
	cancel   func()
	Messages chan []*types.Message
}

func (s *Subscription) Stop() {
	s.cancel()
}

func (s *Subscription) send(msgs []*types.Message) bool {
	select {
	case <-s.context.Done():
		return true
	case s.Messages <- msgs:
		return false
	}
}

func NewMultiplexer(rb ReliableBroadcast) Multiplexer {
	return Multiplexer{
		subscribe: make(chan *Subscription),
		messages:  rb.Watch(),
	}
}

type Multiplexer struct {
	subscribe chan *Subscription
	messages  <-chan []*types.Message
}

func (m Multiplexer) Subscribe(ctx context.Context) (*Subscription, error) {
	ctx, cancel := context.WithCancel(ctx)
	sub := &Subscription{
		context:  ctx,
		cancel:   cancel,
		Messages: make(chan []*types.Message, 1),
	}
	select {
	case m.subscribe <- sub:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return sub, nil
}

func (m Multiplexer) Run(ctx context.Context) error {
	subs := map[*Subscription]struct{}{}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sub := <-m.subscribe:
			subs[sub] = struct{}{}
		case msgs := <-m.messages:
			for sub := range subs {
				if sub.send(msgs) {
					delete(subs, sub)
				}
			}
		}
	}
}
