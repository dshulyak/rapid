package grpc

import (
	"context"
	"fmt"

	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/network/grpc/service"
	"github.com/dshulyak/rapid/types"

	"google.golang.org/grpc"
)

type broadcastServer func(context.Context, []*types.Message) error

func (bs broadcastServer) Send(ctx context.Context, b *service.Batch) (*service.BatchResponse, error) {
	return &service.BatchResponse{}, bs(ctx, b.Messages)
}

type broadcastClient struct {
	conn   *grpc.ClientConn
	client service.BroadcasterClient
}

func (bc broadcastClient) Send(ctx context.Context, msgs []*types.Message) error {
	_, err := bc.client.Send(ctx, &service.Batch{Messages: msgs})
	return err
}

func (bc broadcastClient) Close() error {
	return bc.conn.Close()
}

func (n GRPCNetwork) RegisterBroadcasterServer(f func(context.Context, []*types.Message) error) {
	service.RegisterBroadcasterServer(n.srv, broadcastServer(f))
}

func (n GRPCNetwork) BroadcasterClient(ctx context.Context, node *types.Node) (network.Connection, error) {
	addr := fmt.Sprintf("%s:%d", node.IP, node.Port)
	ctx, cancel := context.WithTimeout(ctx, n.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %v: %w", node, err)
	}
	return broadcastClient{conn, service.NewBroadcasterClient(conn)}, nil
}
