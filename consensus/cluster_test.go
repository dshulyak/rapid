package consensus_test

import (
	"context"
	"math/rand"
	"time"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/swarms/inproc"
	"github.com/dshulyak/rapid/consensus/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	atypes "github.com/dshulyak/rapid/types"
)

func NewCluster(n int, tick time.Duration, jitter int64) *Cluster {
	logger := testLogger()
	network := inproc.NewNetwork()

	conf := &atypes.Configuration{}
	for i := 1; i <= n; i++ {
		conf.Nodes = append(conf.Nodes, &atypes.Node{ID: uint64(i)})
	}
	managers := map[uint64]*consensus.Manager{}
	instanceID := make([]byte, 4)
	rand.Read(instanceID)
	for i := 1; i <= n; i++ {
		conf := consensus.Config{
			Timeout:          8,
			HeartbeatTimeout: 2,
			ReplicaID:        uint64(i),
			FastQuorum:       3 * n / 4,
			ClassicQuorum:    n/2 + 1,
			InstanceID:       instanceID,
			Configuration:    conf,
		}
		pax := consensus.NewPaxos(logger, conf)
		tick := tick + time.Duration(rand.Int63n(jitter))*time.Millisecond
		cons := consensus.NewConsensus(logger.With("node", uint64(i)), pax, tick)
		swarm := inproc.NewSwarm(logger, network, uint64(i))
		managers[uint64(i)] = consensus.NewManager(logger.With("node", uint64(i)), cons, swarm)
	}

	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)
	return &Cluster{
		group:    group,
		ctx:      ctx,
		cancel:   cancel,
		logger:   logger,
		size:     n,
		network:  network,
		managers: managers,
	}
}

type Cluster struct {
	group  *errgroup.Group
	ctx    context.Context
	cancel func()

	logger *zap.SugaredLogger

	size    int
	network *inproc.Network

	managers map[uint64]*consensus.Manager
}

func (c *Cluster) Start() {
	for i := range c.managers {
		m := c.managers[i]
		c.group.Go(func() error {
			return m.Run(c.ctx)
		})
	}
}

func (c *Cluster) Stop() error {
	c.cancel()
	return c.group.Wait()
}

func (c *Cluster) Manager(id uint64) *consensus.Manager {
	return c.managers[id]
}

func (c *Cluster) Propose(ctx context.Context, value *types.Value) error {
	group, ctx := errgroup.WithContext(ctx)
	for _, m := range c.managers {
		m := m
		group.Go(func() error {
			return m.Propose(ctx, value)
		})
	}
	return group.Wait()
}

func (c *Cluster) Subscribe(ctx context.Context, values chan<- []*types.LearnedValue) {
	for _, m := range c.managers {
		m.Subscribe(ctx, values)
	}
}
