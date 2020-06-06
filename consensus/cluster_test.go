package consensus_test

import (
	"context"
	"math/rand"
	"time"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/network/inproc"
	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func NewCluster(n int, tick time.Duration, jitter int64) *Cluster {
	logger := testLogger()
	net := inproc.NewNetwork()

	conf := &types.Configuration{}
	for i := 1; i <= n; i++ {
		conf.Nodes = append(conf.Nodes, &types.Node{ID: uint64(i)})
	}
	configurations := map[uint64]*types.LastConfiguration{}
	reactors := map[uint64]*consensus.Reactor{}
	for i := 1; i <= n; i++ {
		id := uint64(i)
		conf := consensus.Config{
			Timeout:       4,
			Node:          conf.Nodes[i-1],
			Configuration: conf,
			Period:        tick + time.Duration(rand.Int63n(jitter))*time.Millisecond,
		}
		last := types.Last(conf.Configuration)
		configurations[id] = last
		logger := logger.With("node", id)
		reactors[id] = consensus.NewReactor(logger,
			network.NewBroadcastFacade(
				network.NewReliableBroadcast(
					logger,
					net.BroadcastNetwork(id),
					last,
					network.Config{
						NodeID:      id,
						Fanout:      2,
						DialTimeout: time.Second,
						SendTimeout: time.Second,
						RetryPeriod: time.Second,
					},
				)),
			last,
			consensus.NewPaxos(logger, conf),
			conf.Period)
	}

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)
	return &Cluster{
		group:          group,
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		nodes:          conf.Nodes,
		size:           n,
		network:        net,
		reactors:       reactors,
		configurations: configurations,
	}
}

type Cluster struct {
	group  *errgroup.Group
	ctx    context.Context
	cancel func()

	logger *zap.SugaredLogger

	nodes []*types.Node

	size    int
	network *inproc.Network

	configurations map[uint64]*types.LastConfiguration
	reactors       map[uint64]*consensus.Reactor
}

func (c *Cluster) Network() *inproc.Network {
	return c.network
}

func (c *Cluster) Start() {
	for i := range c.reactors {
		r := c.reactors[i]
		c.group.Go(func() error {
			return r.Run(c.ctx)
		})
	}
}

func (c *Cluster) Stop() error {
	c.cancel()
	return c.group.Wait()
}

func (c *Cluster) Reactor(id uint64) *consensus.Reactor {
	return c.reactors[id]
}

func (c *Cluster) Last(id uint64) *types.LastConfiguration {
	return c.configurations[id]
}

func (c *Cluster) Iterate(f func(id uint64) bool) {
	for _, n := range c.nodes {
		if !f(n.ID) {
			return
		}
	}
}

func (c *Cluster) Nodes() []*types.Node {
	return c.nodes
}

func (c *Cluster) Propose(ctx context.Context, value *types.Value) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, r := range c.reactors {
		r := r
		group.Go(func() error {
			return r.Propose(ctx, value)
		})
	}
	return group.Wait()
}
