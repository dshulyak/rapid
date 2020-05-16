package grpc

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/dshulyak/rapid/bootstrap"
	bgrpc "github.com/dshulyak/rapid/bootstrap/network/grpc"
	"github.com/dshulyak/rapid/consensus"
	cgrpc "github.com/dshulyak/rapid/consensus/network/grpc"
	"github.com/dshulyak/rapid/monitor"
	mgrpc "github.com/dshulyak/rapid/monitor/network/grpc"
	"github.com/dshulyak/rapid/types"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func New(logger *zap.Logger, dialTimeout, sendTimeout time.Duration) GRPCNetwork {
	opts := []grpc_zap.Option{
		grpc_zap.WithLevels(grpc_zap.DefaultCodeToLevel),
	}
	grpc_zap.ReplaceGrpcLoggerV2(logger)

	return GRPCNetwork{
		logger:      logger.Sugar(),
		dialTimeout: dialTimeout,
		sendTimeout: sendTimeout,
		srv: grpc.NewServer(
			grpc_middleware.WithUnaryServerChain(
				grpc_ctxtags.UnaryServerInterceptor(),
				grpc_zap.UnaryServerInterceptor(logger, opts...),
			),
			grpc_middleware.WithStreamServerChain(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, opts...),
			),
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

func (n GRPCNetwork) BootstrapServer() bootstrap.NetworkServer {
	return bgrpc.NewService(n.srv)
}

func (n GRPCNetwork) BootstrapClient() bootstrap.NetworkClient {
	return bgrpc.NewClient(n.dialTimeout, n.sendTimeout)
}

func (n GRPCNetwork) ConsensusNetworkService(configuration *types.Configuration) consensus.NetworkService {
	return cgrpc.New(n.logger, n.srv, configuration, n.dialTimeout, n.sendTimeout)
}

func (n GRPCNetwork) MonitorNetworkService(configuration *types.Configuration, node *types.Node) monitor.NetworkService {
	return mgrpc.New(n.logger, node.ID, n.srv, n.dialTimeout, n.sendTimeout)
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
