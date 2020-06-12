package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func New(logger *zap.Logger, dialTimeout, sendTimeout time.Duration) GRPCNetwork {
	return GRPCNetwork{
		logger:      logger.Sugar(),
		dialTimeout: dialTimeout,
		sendTimeout: sendTimeout,
		srv: grpc.NewServer(
			grpc.KeepaliveParams(keepalive.ServerParameters{
				MaxConnectionIdle: 10 * time.Minute,
				Time:              30 * time.Minute,
				Timeout:           30 * time.Second,
			}),
		),
	}
}

type GRPCNetwork struct {
	logger *zap.SugaredLogger

	dialTimeout, sendTimeout time.Duration

	srv *grpc.Server
}

func (n GRPCNetwork) Listen(ctx context.Context, node *types.Node) error {
	errchan := make(chan error, 1)
	go func() {
		sock, err := net.Listen("tcp", fmt.Sprintf("%s:%d", node.IP, node.Port))
		if err != nil {
			errchan <- err
			return
		}
		n.logger.With("address", sock.Addr()).Info("started grpc")
		errchan <- n.srv.Serve(sock)
		return
	}()
	for {
		select {
		case <-ctx.Done():
			n.srv.Stop()
		case err := <-errchan:
			return err
		}
	}
}
