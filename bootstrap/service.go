package bootstrap

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

func NewClient(logger *zap.SugaredLogger, seeds []*types.Node, netclient NetworkClient) Client {
	return Client{
		logger:    logger.Named("bootstrap client"),
		seeds:     seeds,
		netclient: netclient,
	}
}

type Client struct {
	logger *zap.SugaredLogger
	seeds  []*types.Node

	netclient NetworkClient
}

func (c Client) Join(ctx context.Context, id uint64) (*types.Configuration, error) {
	group, ctx := errgroup.WithContext(ctx)
	configurations := make(chan *types.Configuration, len(c.seeds))
	// TODO it will be enough to get responses from majority to guarantee that we got recent configuration
	for _, n := range c.seeds {
		n := n
		c.logger.With("node", n).Debug("request configuration")
		group.Go(func() error {
			conf, err := c.netclient.Configuration(ctx, n)
			if err != nil {
				c.logger.With(
					"node", n,
					"error", err,
				).Error("join failed")
				return err
			}
			configurations <- conf
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	close(configurations)
	var max *types.Configuration
	for conf := range configurations {
		if max == nil || max.ID < conf.ID {
			max = conf
		}
	}
	return max, nil
}
