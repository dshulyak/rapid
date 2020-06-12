package monitor

import (
	"context"
	"errors"
	"sync"

	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

type FailureDetector interface {
	Monitor(context.Context, *types.Node) error
}

func NewMonitor(logger *zap.SugaredLogger,
	id uint64,
	last LastKG,
	fd FailureDetector,
	bf network.BroadcastFacade,
	ar AlertsReactor,
) *Monitor {
	return &Monitor{
		logger:   logger.Named("failure detector").With("node", id),
		id:       id,
		fd:       fd,
		bf:       bf,
		ar:       ar,
		last:     last,
		observed: make(chan *types.Message, 1),
	}
}

type Monitor struct {
	logger *zap.SugaredLogger
	id     uint64
	fd     FailureDetector
	bf     network.BroadcastFacade
	ar     AlertsReactor
	last   LastKG

	observed chan *types.Message
}

// Run monitors subjects from the KGraph.
// Failure detection logic is uncapsulated in the FailureDetector component,
// once it fires - detection is irreversible and will be propagated accross the network.
// Will exit only if interrupted by context.
func (m *Monitor) Run(ctx context.Context) error {
	var (
		group sync.WaitGroup
		// fds is a map with cancellation functions
		topology                     = map[uint64]func(){}
		graph, configuration, update = m.last.Last()
	)
	m.change(ctx, &group, topology, graph, configuration)
	defer group.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-m.observed:
			msg = types.WithInstance(configuration.ID, msg)
			msgs := []*types.Message{msg}
			if err := m.ar.Observe(ctx, msgs); err != nil {
				continue
			}
			select {
			case <-ctx.Done():
			case m.bf.Egress() <- msgs:
			}
		case <-update:
			graph, configuration, update = m.last.Last()
			m.change(ctx, &group, topology, graph, configuration)
		}
	}
}

func (m *Monitor) change(ctx context.Context,
	group *sync.WaitGroup,
	topology map[uint64]func(),
	kg *graph.KGraph,
	configuration *types.Configuration,
) {
	for id := range topology {
		old := true
		kg.IterateSubjects(m.id, func(n *types.Node) bool {
			if n.ID == id {
				old = false
				return false
			}
			return true
		})
		if old {
			topology[id]()
			delete(topology, id)
		}
	}
	kg.IterateSubjects(m.id, func(node *types.Node) bool {
		ctx, cancel := context.WithCancel(ctx)
		_, exist := topology[node.ID]
		if exist {
			return true
		}
		topology[node.ID] = cancel
		group.Add(1)

		go func(node *types.Node) {
			defer group.Done()
			logger := m.logger.With("peer", node.ID)
			logger.Debug("started failure detector instance")
			err := m.fd.Monitor(ctx, node)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Debug("failure detector interrupted")
					return
				}
				logger.Info("detected failure")

				change := &types.Change{
					Type: types.Change_REMOVE,
					Node: node,
				}
				select {
				case m.observed <- types.NewAlert(m.id, node.ID, change):
				case <-ctx.Done():
				}
			}
		}(node)
		return true
	})
}
