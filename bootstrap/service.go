package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

var (
	// ErrNodeIDConflict raised if id conflicts with another node in the pending or commited set.
	ErrNodeIDConflict = errors.New("chosen id is already taken by another node")
)

type NetworkServer interface {
	Register(*Service)
}

type NetworkClient interface {
	Configuration(context.Context, *types.Node) (*types.Configuration, error)
}

func NewService(logger *zap.SugaredLogger, configuration *types.Configuration, netsrv NetworkServer) *Service {
	svc := &Service{
		logger: logger,
	}
	svc.Update(configuration)
	netsrv.Register(svc)
	return svc
}

type Service struct {
	logger *zap.SugaredLogger

	config atomic.Value
}

func (s *Service) Update(configuration *types.Configuration) {
	s.config.Store(configuration)
}

func (s *Service) Configuration() *types.Configuration {
	return s.config.Load().(*types.Configuration)
}

func NewClient(logger *zap.SugaredLogger, seed *types.Node, netclient NetworkClient) Client {
	return Client{
		logger:    logger.Named("bootstrap client"),
		seed:      seed,
		netclient: netclient,
	}
}

type Client struct {
	logger *zap.SugaredLogger
	seed   *types.Node

	netclient NetworkClient
}

func (c Client) Join(ctx context.Context) (*types.Configuration, error) {
	return c.netclient.Configuration(ctx, c.seed)
}
