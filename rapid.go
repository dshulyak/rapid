package rapid

import (
	"context"
	"encoding/json"
	"math/rand"
	"sort"
	"time"

	"github.com/dshulyak/rapid/bootstrap"
	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/network/grpc"
	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type StringDuration time.Duration

func (d StringDuration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *StringDuration) UnmarshalJSON(b []byte) error {
	v := ""
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	val, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	*d = StringDuration(val)
	return nil
}

type Config struct {
	// Expected network delay. Used as a unit for ticks.
	NetworkDelay StringDuration

	// Paxos

	// Timeout after value was proposed, replica will wait for Timeout
	// before executing fallback with classic paxos
	Timeout int

	// Monitoring

	// Each observer for the same subject must reinforce other observer vote after reinforce timeout.
	ReinforceTimeout int

	// Connectivity is a K paramter, used for monitoring topology construction.
	// Each node will have K observers and K subjects.
	Connectivity  int
	LowWatermark  int
	HighWatermark int

	// Network
	BroadcastFanout          int
	RetryPeriod              StringDuration
	DialTimeout, SendTimeout StringDuration

	// Bootstrap
	JoinTries   int
	JoinTimeout StringDuration

	Seed *types.Node

	IP   string
	Port uint64
}

type Network interface {
	network.Network
	bootstrap.Network
	Listen(ctx context.Context, node *types.Node) error
}

// TODO maybe replace logger with interface
func New(
	logger *zap.Logger,
	conf Config,
	fd monitor.FailureDetector,
) Rapid {
	return Rapid{
		logger: logger.Sugar().Named("rapid"),
		conf:   conf,
		fd:     fd,
		network: grpc.New(
			logger,
			time.Duration(conf.DialTimeout),
			time.Duration(conf.SendTimeout),
		),
		rng:  rand.New(rand.NewSource(time.Now().Unix())),
		last: types.Last(nil),
	}
}

type Rapid struct {
	logger *zap.SugaredLogger

	conf    Config
	fd      monitor.FailureDetector
	network Network
	rng     *rand.Rand

	last *types.LastConfiguration
}

func (r Rapid) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	node := &types.Node{
		IP:   r.conf.IP,
		Port: r.conf.Port,
		ID:   r.rng.Uint64(),
	}

	seed := Compare(node, r.conf.Seed)
	if seed {
		r.logger.With("node", node).Info("bootstrapping a cluster")
	} else {
		r.logger.With("node", node).Info("joining cluster")
	}
	var (
		configuration *types.Configuration
		err           error
	)

	if !seed {
		for i := 0; i < r.conf.JoinTries; i++ {
			configuration, err = r.join(ctx, node)
			if err != nil {
				return err
			}
			break
		}
	} else {
		configuration = &types.Configuration{
			Nodes: []*types.Node{node},
		}
	}

	r.last.Update(configuration)
	r.logger.Info("starting the cluster")
	broadcaster := network.NewBroadcastFacade(
		network.NewReliableBroadcast(
			r.logger,
			r.network,
			r.last,
			network.Config{
				NodeID:      node.ID,
				Fanout:      r.conf.BroadcastFanout,
				DialTimeout: time.Duration(r.conf.DialTimeout),
				SendTimeout: time.Duration(r.conf.SendTimeout),
				RetryPeriod: time.Duration(r.conf.RetryPeriod),
			},
		),
	)

	mon := monitor.NewManager(
		r.logger,
		monitor.Config{
			Node:             node,
			K:                r.conf.Connectivity,
			LW:               r.conf.LowWatermark,
			HW:               r.conf.HighWatermark,
			TimeoutPeriod:    time.Duration(r.conf.NetworkDelay),
			ReinforceTimeout: r.conf.ReinforceTimeout,
		},
		r.last,
		r.fd,
		broadcaster,
	)

	tick := time.Duration(r.conf.NetworkDelay) + time.Duration(r.rng.Int63n(int64(r.conf.NetworkDelay)/2))
	cons := consensus.NewReactor(
		r.logger,
		broadcaster,
		r.last,
		consensus.NewPaxos(r.logger, consensus.Config{
			Node:          node,
			Configuration: configuration,
			Timeout:       r.conf.Timeout,
		}),
		tick,
	)

	// New registers service in the network
	// maybe API is a bit confusing
	_ = bootstrap.New(r.logger, node.ID, r.last, r.network, mon, broadcaster)

	group.Go(func() error {
		defer r.logger.Info("network listener exited")
		return r.network.Listen(ctx, node)
	})
	group.Go(func() error {
		defer r.logger.Info("broadcaster exited")
		return broadcaster.Run(ctx)
	})
	group.Go(func() error {
		defer r.logger.Info("monitoring overlay exited")
		return mon.Run(ctx)
	})
	group.Go(func() error {
		defer r.logger.Info("consensus engine exited")
		return cons.Run(ctx)
	})
	// TODO pass channel for proposals directly to alerts reactor
	// changeset must be sorted before posting to that channel
	group.Go(func() error {
		for changeset := range mon.Changes() {
			// equality depends on the order
			sort.Slice(changeset, func(i, j int) bool {
				return changeset[i].Node.ID < changeset[j].Node.ID
			})
			if err := cons.Propose(ctx, &types.Value{Changes: changeset}); err != nil {
				return err
			}
		}
		return nil
	})

	return group.Wait()
}

func (r Rapid) join(ctx context.Context, node *types.Node) (*types.Configuration, error) {
	client := bootstrap.NewClient(r.logger, r.network)
	configuration, err := client.GetConfiguration(ctx, []*types.Node{r.conf.Seed})
	if err != nil {
		r.logger.With("error", err).Error("requesting configuration from seed failed")
		return nil, err
	}
	r.logger.With(
		"configuration", configuration,
	).Info("got configuration from seeds")

	graph := graph.New(r.conf.Connectivity, configuration.Nodes)
	observers := []*types.Node{}
	group, gctx := errgroup.WithContext(ctx)
	graph.IterateObservers(node.ID, func(peer *types.Node) bool {
		peer = peer
		observers = append(observers, peer)
		group.Go(func() error {
			return client.Join(gctx, configuration.ID, node, peer)
		})
		return true
	})
	if err := group.Wait(); err != nil {
		return nil, err
	}
	r.logger.Info("requested to join cluster")

	ctx, cancel := context.WithTimeout(ctx, time.Duration(r.conf.JoinTimeout))
	defer cancel()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			configuration, err = client.GetConfiguration(ctx, observers)
			if err != nil {
				continue
			}
			for _, other := range configuration.Nodes {
				if node.ID == other.ID && Compare(node, other) {
					return configuration, nil
				}
			}
		}
	}
}

func (r Rapid) Configuration() (*types.Configuration, <-chan struct{}) {
	return r.last.Last()
}

func Compare(a, b *types.Node) bool {
	return a.IP == b.IP && a.Port == b.Port
}
