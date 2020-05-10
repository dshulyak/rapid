package rapid

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"time"

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

var (
	// ErrInvalidSeed raised if node must be in the seed list but it is not.
	ErrInvalidSeed = errors.New("invalid seed list")
)

func NewSeed(logger *zap.SugaredLogger, conf Config, fd monitor.FailureDetector) RapidSeed {
	return RapidSeed{
		logger: logger.Named("rapid seed"),
		conf:   conf,
		fd:     fd,
	}
}

type RapidSeed struct {
	logger *zap.SugaredLogger
	conf   Config
	fd     monitor.FailureDetector
}

func (r RapidSeed) Run(ctx context.Context, updates chan *types.Configuration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	var node *types.Node
	for _, n := range r.conf.Seeds {
		if n.IP == r.conf.IP && n.Port == r.conf.Port {
			node = n
			break
		}
	}
	if node == nil {
		return fmt.Errorf("%w: node %s:%d not in the seed list", ErrInvalidSeed, r.conf.IP, r.conf.Port)
	}

	configuration := &types.Configuration{
		Nodes: r.conf.Seeds,
	}

	r.logger.With("node", node).Info("instance is launched as seed")

	srv := grpc.NewServer()

	cons := consensus.NewManager(
		r.logger,
		cgrpc.New(r.logger, srv, configuration),
		consensus.Config{
			Node:             node,
			Configuration:    configuration,
			Timeout:          r.conf.ElectionTimeout,
			HeartbeatTimeout: r.conf.HeartbeatTimeout,
		},
		time.Duration(r.conf.NetworkDelay),
	)
	values := make(chan []*ctypes.LearnedValue, 1)
	cons.Subscribe(ctx, values)

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
		mgrpc.New(r.logger, node.ID, srv, time.Duration(r.conf.DialTimeout), time.Duration(r.conf.SendTimeout)),
	)

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
