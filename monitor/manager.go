package monitor

import (
	"context"
	"errors"
	"sync/atomic"

	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	ErrOutdatedConfigID = errors.New("outdated configuration")
)

type NetworkHandler struct {
	logger *zap.SugaredLogger
	id     uint64

	configID uint64

	alerts AlertsReactor
}

func (net *NetworkHandler) Join(ctx context.Context, configID uint64, node *types.Node) error {
	local := atomic.LoadUint64(&net.configID)
	if local != configID {
		net.logger.With(
			"node", node,
			"node config", configID,
			"local", local,
		).Debug("outdated configuration")
		return ErrOutdatedConfigID
	}
	return net.alerts.Observe(ctx, &mtypes.Alert{
		Observer: net.id,
		Subject:  node.ID,
		Change: &types.Change{
			Type: types.Change_JOIN,
			Node: node,
		},
	})
}

func (net *NetworkHandler) Broadcast(ctx context.Context, alerts []*mtypes.Alert) error {
	for _, alerts := range alerts {
		if err := net.alerts.Observe(ctx, alerts); err != nil {
			return err
		}
	}
	return nil
}

func (net *NetworkHandler) UpdateConfigID(id uint64) {
	atomic.StoreUint64(&net.configID, id)
}

type NetworkService interface {
	Register(*NetworkHandler)

	// NOTE broadcasting topology depends on the kgraph
	Update(*KGraph)
	Broadcast(context.Context, <-chan []*mtypes.Alert) error

	Join(ctx context.Context, configID uint64, observer, subject *types.Node) error
}

func NewManager(logger *zap.SugaredLogger, conf Config, cluster *types.Configuration, fd FailureDetector, netsvc NetworkService) *Manager {
	kg := NewKGraph(conf.K, cluster.Nodes)
	am := NewAlertsReactor(logger, conf.TimeoutPeriod, NewAlerts(logger, kg, conf))
	mon := NewMonitor(logger, conf.Node.ID, fd, am)
	handler := &NetworkHandler{
		logger: logger.Named("monitor network"),
		id:     conf.Node.ID,
		alerts: am,
	}
	netsvc.Register(handler)
	return &Manager{
		logger:   logger.Named("monitor"),
		conf:     conf,
		mon:      mon,
		alerts:   am,
		handler:  handler,
		network:  netsvc,
		configID: cluster.ID,
		kg:       kg,
	}
}

type Manager struct {
	logger *zap.SugaredLogger
	conf   Config

	kg       *KGraph
	configID uint64

	mon *Monitor

	alerts AlertsReactor

	handler *NetworkHandler
	network NetworkService
}

func (m *Manager) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer m.logger.Info("closed alerts reactor")
		return m.alerts.Run(ctx)
	})
	group.Go(func() error {
		defer m.logger.Info("closed broadcaster")
		return m.network.Broadcast(ctx, m.alerts.Alerts())
	})
	group.Go(func() error {
		defer m.logger.Info("closed monitoring extension")
		return m.mon.Run(ctx)
	})
	return group.Wait()
}

func (m *Manager) Changes() <-chan []*types.Change {
	return m.alerts.Changes()
}

func (m *Manager) Update(cluster *types.Configuration) {
	m.logger.With("ID", cluster.ID).Debug("updating monitoring")
	kg := NewKGraph(m.conf.K, cluster.Nodes)
	m.alerts.Update(kg)
	m.mon.Update(kg)
	m.network.Update(kg)
	m.handler.UpdateConfigID(cluster.ID)
	m.kg = kg
	m.configID = cluster.ID
	m.logger.With("ID", cluster.ID).Debug("finished update")
}

func (m *Manager) Join(ctx context.Context) (err error) {
	sent := map[uint64]struct{}{}
	m.kg.IterateObservers(m.conf.Node.ID, func(n *types.Node) bool {
		// supress notification on duplicate edges
		if _, exist := sent[n.ID]; exist {
			return true
		}
		sent[n.ID] = struct{}{}
		err = m.network.Join(ctx, m.configID, n, m.conf.Node)
		if err != nil {
			return false
		}
		return true
	})
	return err
}
