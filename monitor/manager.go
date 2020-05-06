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
	logger *zap.Logger
	id     uint64

	configID uint64

	alerts *AlertsReactor
}

func (net NetworkHandler) Join(ctx context.Context, configID uint64, node *types.Node) error {
	local := atomic.LoadUint64(&net.configID)
	if local != configID {
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

func (net NetworkHandler) Broadcast(ctx context.Context, alert *mtypes.Alert) error {
	return net.alerts.Observe(ctx, alert)
}

func (net NetworkHandler) UpdateConfigID(id uint64) {
	atomic.StoreUint64(&net.configID, id)
}

type Network interface {
	Register(NetworkHandler)

	// NOTE broadcasting topology depends on the kgraph, it won't be required
	Update(*KGraph)
	Broadcast(context.Context, <-chan []*mtypes.Alert) error

	Join(ctx context.Context, configID uint64, observer, subject *types.Node) error
}

type Manager struct {
	conf   Config
	logger *zap.SugaredLogger

	kg *KGraph

	mon *Monitor

	alerts *AlertsReactor

	handler NetworkHandler
	network Network
}

func (m Manager) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return m.alerts.Run(ctx)
	})
	group.Go(func() error {
		return m.network.Broadcast(ctx, m.alerts.Alerts())
	})
	group.Go(func() error {
		return m.mon.Run(ctx)
	})
	return group.Wait()
}

func (m Manager) Changes() <-chan []*types.Change {
	return m.alerts.Changes()
}

func (m Manager) Update(conf *types.Configuration) {
	kg := NewKGraph(m.conf.K, conf.Nodes)
	m.alerts.Update(kg)
	m.mon.Update(kg)
	m.network.Update(kg)
	m.handler.UpdateConfigID(conf.ID)
}
