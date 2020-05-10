package rapid

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/dshulyak/rapid/bootstrap"
	bgrpc "github.com/dshulyak/rapid/bootstrap/network/grpc"
	"github.com/dshulyak/rapid/consensus"
	cgrpc "github.com/dshulyak/rapid/consensus/network/grpc"
	ctypes "github.com/dshulyak/rapid/consensus/types"
	"github.com/dshulyak/rapid/monitor"
	mgrpc "github.com/dshulyak/rapid/monitor/network/grpc"
	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

type Config struct {
	// Expected network delay used for ticks
	NetworkDelay time.Duration

	// Paxos

	// Timeouts are expressed in number of network delays.
	// ElectionTimeout if replica doesn't receive hearbeat for ElectionTimeout ticks
	// it will start new election, by sending Prepare to other replicas.
	ElectionTimeout int
	// HeartbeatTimeout must be lower than election timeout.
	// Leader will send last sent Accept message to every replica as a heartbeat.
	HeartbeatTimeout int

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

	Seeds []*types.Node

	IP   string
	Port uint64

	DialTimeout, SendTimeout time.Duration
}

// TODO replace logger with interface and allow to initialize Rapid with factory for creating network backends.
func New(
	logger *zap.SugaredLogger,
	conf Config,
	fd monitor.FailureDetector,
) Rapid {
	return Rapid{
		logger: logger.Named("rapid"),
		conf:   conf,
		fd:     fd,
	}
}

type Rapid struct {
	logger *zap.SugaredLogger

	conf Config
	fd   monitor.FailureDetector
}

func (r Rapid) Run(ctx context.Context, updates chan<- *types.Configuration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	rng := rand.New(rand.NewSource(time.Now().Unix()))
	node := &types.Node{
		IP:   r.conf.IP,
		Port: r.conf.Port,
		ID:   rng.Uint64(),
	}

	r.logger.With("node", node).Info("joining cluster")

	srv := grpc.NewServer()

	configuration, err := r.bootstrapClient().Join(ctx, node.ID)
	if err != nil {
		r.logger.With("error", err).Error("step 1 join failed")
		return err
	}
	r.logger.With("configuration", configuration).Debug("step 1 join succeeded. received configuration")

	cons := consensus.NewManager(
		r.logger,
		cgrpc.New(r.logger, srv, configuration),
		consensus.Config{
			Node:             node,
			Configuration:    configuration,
			Timeout:          r.conf.ElectionTimeout,
			HeartbeatTimeout: r.conf.HeartbeatTimeout,
		},
		r.conf.NetworkDelay,
	)

	mon := monitor.NewManager(
		r.logger,
		monitor.Config{
			Node:              node,
			K:                 r.conf.Connectivity,
			LW:                r.conf.LowWatermark,
			HW:                r.conf.HighWatermark,
			TimeoutPeriod:     r.conf.NetworkDelay,
			ReinforceTimeout:  r.conf.ReinforceTimeout,
			RetransmitTimeout: r.conf.RetransmitTimeout,
		},
		configuration,
		r.fd,
		mgrpc.New(r.logger, node.ID, srv, r.conf.DialTimeout, r.conf.SendTimeout),
	)

	values := make(chan []*ctypes.LearnedValue, 1)
	cons.Subscribe(ctx, values)

	group.Go(func() error {
		sock, err := net.Listen("tcp", fmt.Sprintf("%s:%d", node.IP, node.Port))
		if err != nil {
			return err
		}
		return srv.Serve(sock)
	})

	group.Go(func() error {
		<-ctx.Done()
		srv.Stop()
		return ctx.Err()
	})

	group.Go(func() error {
		return cons.Run(ctx)
	})

	if err := mon.Join(ctx); err != nil {
		r.logger.With("error", err).Error("step 2 join failed")
	}
	r.logger.Debug("step 2 join suceeded")

	configuration, err = r.waitJoined(ctx, node.ID, values)
	if err != nil {
		return err
	}
	r.logger.With("configuration", configuration, "id", node.ID).Info("node joined the cluster")

	mon.Update(configuration)

	group.Go(func() error {
		return mon.Run(ctx)
	})

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
					"id", sum,
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

func (r Rapid) waitJoined(ctx context.Context, id uint64, values <-chan []*ctypes.LearnedValue) (*types.Configuration, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case vals := <-values:
			for _, v := range vals {
				for _, change := range v.Value.Changes.List {
					if change.Type == types.Change_JOIN && change.Node.ID == id {
						return &types.Configuration{
							Nodes: v.Value.Nodes,
							ID:    v.Sequence,
						}, nil
					}
				}
			}
		}
	}
}

func (r Rapid) bootstrapClient() bootstrap.Client {
	return bootstrap.NewClient(
		r.logger,
		r.conf.Seeds,
		bgrpc.NewClient(r.conf.DialTimeout, r.conf.SendTimeout),
	)
}
