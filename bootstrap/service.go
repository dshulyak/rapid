package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
	Join(context.Context, *types.Node, uint64) (*types.Configuration, error)
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

	mu sync.Mutex
	// commited configuration
	configuration *types.Configuration
	// set of taken and pending node ids. reset after each configuration update.
	ids map[uint64]struct{}
}

func (s *Service) Update(configuration *types.Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configuration = configuration
	for id := range s.ids {
		delete(s.ids, id)
	}
	for _, n := range configuration.Nodes {
		s.ids[n.ID] = struct{}{}
	}
}

func (s *Service) Join(ctx context.Context, id uint64) (*types.Configuration, error) {
	logger := s.logger.With("node id", id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.configuration == nil {

	}
	if _, exist := s.ids[id]; exist {
		logger.Debug("id conflict")
		return nil, fmt.Errorf("%w: conflicting id %d", ErrNodeIDConflict, id)
	}
	logger.Debug("id registered")
	s.ids[id] = struct{}{}
	return s.configuration, nil
}

func NewClient(logger *zap.SugaredLogger, seeds []*types.Node, netclient NetworkClient) Client {
	return Client{
		logger:    logger,
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
		group.Go(func() error {
			conf, err := c.netclient.Join(ctx, n, id)
			if err != nil {
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
