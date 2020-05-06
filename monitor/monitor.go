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

func NewMonitor(logger *zap.SugaredLogger, id uint64, fd FailureDetector, am AlertsReactor, kg *KGraph) *Monitor {
	mon := &Monitor{
		logger: logger,
		id:     id,
		fd:     fd,
		am:     am,
		graph:  make(chan *KGraph, 1),
	}
	mon.Update(kg)
	return mon
}

type Monitor struct {
	logger *zap.SugaredLogger
	id     uint64
	fd     FailureDetector
	am     AlertsReactor

	graph chan *KGraph
}

func (m *Monitor) Update(kg *KGraph) {
	m.graph <- kg
}

// Run monitors subjects from the KGraph.
// Failure detection logic is uncapsulated in the FailureDetector component,
// once it fires - detection is irreversible and will be propagated accross the network.
// Will exit only if interrupted by context.
func (m *Monitor) Run(ctx context.Context) error {
	var (
		group sync.WaitGroup
		// fds is a map with cancellation functions
		//fds   = map[uint64]func(){}
	)
	defer group.Wait()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case kg := <-m.graph:
			kg.IterateSubjects(m.id, func(node *types.Node) bool {
				group.Add(1)
				go func(node *types.Node) {
					defer group.Done()
					err := m.fd.Monitor(ctx, node)
					if err != nil && !errors.Is(err, context.Canceled) {
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
	}
}
