package monitor

import (
	"context"
	"errors"
	"sync"

	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

type FailureDetector interface {
	Monitor(context.Context, *types.Node) error
}

func NewMonitor(logger *zap.SugaredLogger, id uint64, last *LastKG, fd FailureDetector, am AlertsReactor) *Monitor {
	return &Monitor{
		logger: logger.Named("monitor").With("node ID", id),
		id:     id,
		fd:     fd,
		am:     am,
		last:   last,
	}
}

type Monitor struct {
	logger *zap.SugaredLogger
	id     uint64
	fd     FailureDetector
	am     AlertsReactor
	last   *LastKG
}

// Run monitors subjects from the KGraph.
// Failure detection logic is uncapsulated in the FailureDetector component,
// once it fires - detection is irreversible and will be propagated accross the network.
// Will exit only if interrupted by context.
func (m *Monitor) Run(ctx context.Context) error {
	var (
		group sync.WaitGroup
		// fds is a map with cancellation functions
		topology      = map[uint64]func(){}
		graph, update = m.last.Last()
	)
	m.change(ctx, &group, topology, graph)
	defer group.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-update:
			graph, update = m.last.Last()
			m.change(ctx, &group, topology, graph)
		}
	}
}

func (m *Monitor) change(ctx context.Context, group *sync.WaitGroup, topology map[uint64]func(), kg *KGraph) {
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
			logger := m.logger.With("node", node)
			logger.Debug("started failure detector instance")
			err := m.fd.Monitor(ctx, node)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					logger.Debug("failure detector interrupted")
					return
				}
				logger.Info("detected failure")
				_ = m.am.Observe(ctx, &mtypes.Alert{
					Observer: m.id,
					Subject:  node.ID,
					Change: &types.Change{
						Type: types.Change_REMOVE,
						Node: node,
					},
				})
			}
		}(node)
		return true
	})
}
