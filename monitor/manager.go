package monitor

import (
	"context"

	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func NewManager(logger *zap.SugaredLogger,
	conf Config,
	configuration *types.LastConfiguration,
	fd FailureDetector,
	bf network.BroadcastFacade,
) *Manager {
	ar := NewAlertsReactor(
		logger,
		conf.TimeoutPeriod,
		configuration,
		bf,
		NewAlerts(logger, configuration.Configuration(), conf),
	)
	mon := NewMonitor(logger,
		conf.Node.ID,
		LastKG{kparam: conf.K, conf: configuration},
		fd,
		bf,
		ar,
	)
	return &Manager{
		logger:  logger.Named("monitor"),
		conf:    conf,
		mon:     mon,
		reactor: ar,
		bf:      bf,
	}
}

type Manager struct {
	logger *zap.SugaredLogger
	conf   Config

	mon     *Monitor
	reactor AlertsReactor
	bf      network.BroadcastFacade
}

func (m *Manager) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer m.logger.Info("alerts reactor is stopped")
		return m.reactor.Run(ctx)
	})
	group.Go(func() error {
		defer m.logger.Info("monitoring is stopped")
		return m.mon.Run(ctx)
	})
	return group.Wait()
}

func (m *Manager) Changes() <-chan []*types.Change {
	return m.reactor.Changes()
}
