package inproc

import (
	"context"
	"fmt"

	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/types"
)

const (
	broadcastCode uint64 = 1
)

type broadcastClient struct {
	from, to uint64
	pipe     *pipe
}

func (c broadcastClient) Send(ctx context.Context, msgs []*types.Message) error {
	return c.pipe.send(Request{
		Context: ctx,
		From:    c.from,
		To:      c.to,
		Code:    broadcastCode,
		Object:  msgs,
	})
}

func (c broadcastClient) Close() error {
	c.pipe.stop()
	return nil
}

type broadcastBridge struct {
	nodeID uint64
	net    *Network
}

func (b broadcastBridge) BroadcasterClient(ctx context.Context, n *types.Node) (network.Connection, error) {
	p, err := b.net.Connect(b.nodeID, n.ID)
	if err != nil {
		return nil, err
	}
	return broadcastClient{from: b.nodeID, to: n.ID, pipe: p}, nil
}

func (b broadcastBridge) RegisterBroadcasterServer(f func(context.Context, []*types.Message) error) {
	b.net.Register(b.nodeID, broadcastCode, func(ctx context.Context, obj interface{}) *Response {
		msgs, ok := obj.([]*types.Message)
		if !ok {
			return &Response{
				Err: fmt.Errorf("unexpected type received as broadcast message: %T", obj),
			}
		}
		return &Response{Err: f(ctx, msgs)}
	})
}
