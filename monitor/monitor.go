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

type AlertManager interface {
	Observe(context.Context, *mtypes.Alert) error
}

func NewMonitor(logger *zap.SugaredLogger, id uint64, fd FailureDetector, am AlertManager, kg *KGraph) *Monitor {
	gchan := make(chan *KGraph, 1)
	gchan <- kg
	return &Monitor{
		logger: logger,
		id:     id,
		fd:     fd,
		am:     am,
		graph:  gchan,
	}
}

type Monitor struct {
	logger *zap.SugaredLogger
	id     uint64
	fd     FailureDetector
	am     AlertManager

	graph chan *KGraph
}

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
