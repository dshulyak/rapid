package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/dshulyak/rapid/bootstrap/network/grpc/service"
	"github.com/dshulyak/rapid/types"
	"google.golang.org/grpc"
)

func NewClient(dialTimeout, sendTimeout time.Duration) Client {
	return Client{
		dialTimeout: dialTimeout,
		sendTimeout: sendTimeout,
	}
}

type Client struct {
	dialTimeout, sendTimeout time.Duration
}

func (c Client) Configuration(ctx context.Context, n *types.Node) (*types.Configuration, error) {
	conn, err := c.dial(ctx, n)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := service.NewBootstrapClient(conn)
	ctx, cancel := context.WithTimeout(ctx, c.sendTimeout)
	defer cancel()
	return client.Configuration(ctx, &service.Empty{})

}

func (c Client) dial(ctx context.Context, n *types.Node) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()
	return grpc.DialContext(ctx, fmt.Sprintf("%s:%d", n.IP, n.Port), grpc.WithInsecure(), grpc.WithBlock())
}
