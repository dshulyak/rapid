package rapid

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/dshulyak/rapid/bootstrap"
	"github.com/dshulyak/rapid/consensus"
	ctypes "github.com/dshulyak/rapid/consensus/types"
	"github.com/dshulyak/rapid/monitor"
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
	// Expected network delay used for ticks
	NetworkDelay StringDuration

	// Paxos

	// Timeouts are expressed in number of network delays.
	// ElectionTimeout if replica doesn't receive hearbeat for ElectionTimeout ticks
	// it will start new election, by sending Prepare to other replicas.
	ElectionTimeout int

	// Monitoring

	// Timeouts are expressed in number of network delays.
	// Each observer for the same subject must reinforce other observer vote after reinforce timeout.
	ReinforceTimeout int
	// RetransmitTimeout used to re-broadcast observed alerts.
	RetransmitTimeout int

	// Connectivity is a K paramter, used for monitoring topology construction.
	// Each node will have K observers and K subjects.
	Connectivity  int
	LowWatermark  int
	HighWatermark int

	Seed *types.Node

	IP   string
	Port uint64

	DialTimeout, SendTimeout StringDuration
}

type Network interface {
	BootstrapServer() bootstrap.NetworkServer
	BootstrapClient() bootstrap.NetworkClient
	ConsensusNetworkService(configuration *types.Configuration) consensus.NetworkService
	MonitorNetworkService(configuration *types.Configuration, node *types.Node) monitor.NetworkService
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
		rng: rand.New(rand.NewSource(time.Now().Unix())),
	}
}

type Rapid struct {
	logger *zap.SugaredLogger

	conf    Config
	fd      monitor.FailureDetector
	network Network
	rng     *rand.Rand
}

func (r Rapid) Run(ctx context.Context, updates chan<- *types.Configuration) error {
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
		bootClient    = r.bootstrapClient()
	)

	if !seed {
		configuration, err = bootClient.Join(ctx)
		if err != nil {
			r.logger.With("error", err).Error("requesting configuration from seed failed")
			return err
		}
	} else {
		configuration = &types.Configuration{
			Nodes: []*types.Node{node},
		}
	}

	r.logger.With(
		"configuration", configuration,
	).Debug("received configuration from seed nodes")

	mon := monitor.NewManager(
		r.logger,
		monitor.Config{
			Node:              node,
			K:                 r.conf.Connectivity,
			LW:                r.conf.LowWatermark,
			HW:                r.conf.HighWatermark,
			TimeoutPeriod:     time.Duration(r.conf.NetworkDelay),
			ReinforceTimeout:  r.conf.ReinforceTimeout,
			RetransmitTimeout: r.conf.RetransmitTimeout,
		},
		configuration,
		r.fd,
		r.network.MonitorNetworkService(configuration, node),
	)

	if !seed {
		r.logger.With("node", node).Debug("joining cluster")
		if err := mon.Join(ctx); err != nil {
			r.logger.With("error", err).Error("step 2 join failed")
			return err
		}

		configuration, err = r.waitJoined(ctx, bootClient, node)
		if err != nil {
			return err
		}
		r.logger.With("configuration", configuration, "node", node).Info("node joined the cluster")
	}

	mon.Update(configuration)

	group.Go(func() error {
		return r.network.Listen(ctx, node)
	})

	group.Go(func() error {
		return mon.Run(ctx)
	})

	cons := consensus.NewManager(
		r.logger,
		r.network.ConsensusNetworkService(configuration),
		consensus.Config{
			Node:          node,
			Configuration: configuration,
			Timeout:       r.conf.ElectionTimeout,
		},
		time.Duration(r.conf.NetworkDelay)+time.Duration(r.rng.Int63n(int64(r.conf.NetworkDelay)/2)),
	)

	group.Go(func() error {
		return cons.Run(ctx)
	})

	boot := bootstrap.NewService(r.logger, configuration, r.network.BootstrapServer())

	values := make(chan []*ctypes.LearnedValue, 1)
	cons.Subscribe(ctx, values)

	group.Go(func() error {
		mapping := map[uint64]*types.Node{}
		for _, node := range configuration.Nodes {
			mapping[node.ID] = node
		}
		for {
			select {
			case vals := <-values:
				for _, v := range vals {
					update := &types.Configuration{
						Nodes: v.Value.Nodes,
						ID:    v.Sequence,
					}
					mon.Update(update)
					boot.Update(update)
					select {
					case updates <- update:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			case changes := <-mon.Changes():
				for _, change := range changes {
					switch change.Type {
					case types.Change_JOIN:
						mapping[change.Node.ID] = change.Node
					case types.Change_REMOVE:
						delete(mapping, change.Node.ID)
					}
				}
				nodes := make([]*types.Node, 0, len(mapping))
				for _, n := range mapping {
					nodes = append(nodes, n)
				}

				changeset := &types.Changes{List: changes}

				bytes, err := changeset.Marshal()
				if err != nil {
					r.logger.With(
						"error", err,
						"changeset", changeset,
					).Error("failed to marshal changeset")
					return err
				}
				sum := sha256.Sum256(bytes)

				r.logger.With(
					"id", hex.EncodeToString(sum[:]),
					"changes", changes,
				).Info("proposing changeset")

				if err := cons.Propose(ctx, &ctypes.Value{
					Id:      sum[:],
					Nodes:   nodes,
					Changes: changeset,
				}); err != nil {
					r.logger.With("error", err).Error("failed to propose")
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	return group.Wait()
}

func (r Rapid) waitJoined(ctx context.Context, bclient bootstrap.Client, node *types.Node) (*types.Configuration, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			configuration, err := bclient.Join(ctx)
			if err == nil {
				for _, other := range configuration.Nodes {
					if Compare(node, other) {
						return configuration, nil
					}
				}
			}
		}
	}
}

func (r Rapid) bootstrapClient() bootstrap.Client {
	return bootstrap.NewClient(
		r.logger,
		r.conf.Seed,
		r.network.BootstrapClient(),
	)
}

func Compare(a, b *types.Node) bool {
	return a.IP == b.IP && a.Port == b.Port
}
