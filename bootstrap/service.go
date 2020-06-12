package bootstrap

import (
	"context"
	"errors"

	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	ErrOldConfiguration = errors.New("configuration is outdated")
	ErrNoConfiguraton   = errors.New("can't fetch configuration")
)

type BootstrapClient interface {
	Configuration(context.Context) (*types.Configuration, error)
	Join(context.Context, uint64, *types.Node) error
}

type Network interface {
	RegisterBootstrap(Service)
	BootstrapClient(context.Context, *types.Node) (BootstrapClient, error)
}

type AlertsMonitor interface {
	Observe(context.Context, []*types.Message) error
}

type Broadcaster interface {
	Send(context.Context, []*types.Message) error
}

func New(logger *zap.SugaredLogger,
	nodeID uint64,
	configuration *types.LastConfiguration,
	net Network,
	am AlertsMonitor,
	bf Broadcaster,
) Service {
	svc := Service{
		logger: logger,
		nodeID: nodeID,
		last:   configuration,
		am:     am,
		bf:     bf,
	}
	net.RegisterBootstrap(svc)
	return svc
}

type Service struct {
	logger *zap.SugaredLogger

	nodeID uint64

	bf Broadcaster
	am AlertsMonitor

	last *types.LastConfiguration
}

func (s Service) Configuration() *types.Configuration {
	return s.last.Configuration()
}

func (s Service) Join(ctx context.Context, instanceID uint64, node *types.Node) error {
	conf := s.last.Configuration()
	if instanceID < conf.ID {
		return ErrOldConfiguration
	}
	msgs := []*types.Message{types.NewAlert(s.nodeID, node.ID, &types.Change{
		Type: types.Change_JOIN,
		Node: node,
	})}
	if err := s.am.Observe(ctx, msgs); err != nil {
		return err
	}
	if err := s.bf.Send(ctx, msgs); err != nil {
		return err
	}
	return nil
}

func NewClient(logger *zap.SugaredLogger, net Network) Client {
	return Client{
		logger: logger.Named("bootstrap client"),
		net:    net,
	}
}

type Client struct {
	logger *zap.SugaredLogger
	net    Network
}

func (c Client) GetConfiguration(ctx context.Context, nodes []*types.Node) (*types.Configuration, error) {
	group, ctx := errgroup.WithContext(ctx)
	rst := make(chan *types.Configuration, len(nodes))
	for i := range nodes {
		node := nodes[i]
		group.Go(func() error {
			client, err := c.net.BootstrapClient(ctx, node)
			if err != nil {
				rst <- nil
				return nil
			}
			conf, err := client.Configuration(ctx)
			if err != nil {
				rst <- nil
				return nil
			}
			rst <- conf
			return nil
		})
	}
	_ = group.Wait()
	close(rst)
	var max *types.Configuration
	for conf := range rst {
		if max == nil || conf.ID > max.ID {
			max = conf
		}
	}
	if max == nil {
		return nil, ErrNoConfiguraton
	}
	return max, nil
}

func (c Client) Join(ctx context.Context, instanceID uint64, node, peer *types.Node) error {
	client, err := c.net.BootstrapClient(ctx, peer)
	if err != nil {
		return err
	}
	return client.Join(ctx, instanceID, node)
}
