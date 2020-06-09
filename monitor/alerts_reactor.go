package monitor

import (
	"context"
	"time"

	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

func NewAlertsReactor(logger *zap.SugaredLogger,
	period time.Duration,
	last *types.LastConfiguration,
	bf network.BroadcastFacade,
	alerts *Alerts,
) AlertsReactor {
	return AlertsReactor{
		logger:   logger.Named("alerts reactor"),
		period:   period,
		last:     last,
		alerts:   alerts,
		bf:       bf,
		observed: make(chan []*types.Message, 1),
		changes:  make(chan []*types.Change, 1),
	}
}

type AlertsReactor struct {
	logger *zap.SugaredLogger
	period time.Duration

	last   *types.LastConfiguration
	alerts *Alerts

	bf network.BroadcastFacade

	observed chan []*types.Message
	changes  chan []*types.Change
}

func (r AlertsReactor) Observe(ctx context.Context, msgs []*types.Message) error {
	select {
	case r.observed <- msgs:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r AlertsReactor) Changes() <-chan []*types.Change {
	return r.changes
}

func (r AlertsReactor) Run(ctx context.Context) error {
	var (
		ticker = time.NewTicker(r.period)

		egress chan<- []*types.Message
		chchan chan []*types.Change

		configuration, update = r.last.Last()
		sub, err              = r.bf.Subscribe(ctx)
	)
	if err != nil {
		return err
	}
	defer sub.Stop()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.alerts.Tick()
		case <-ctx.Done():
			return ctx.Err()
		case msgs := <-sub.Messages:
			for _, msg := range msgs {
				r.alerts.Observe(msg)
			}
		case msgs := <-r.observed:
			for _, msg := range msgs {
				r.alerts.Observe(msg)
			}
		case chchan <- r.alerts.Changes:
			chchan = nil
			r.alerts.Changes = nil
		case egress <- r.alerts.Messages:
			egress = nil
			r.alerts.Messages = nil
		case <-update:
			configuration, update = r.last.Last()
			r.alerts.Update(configuration)
		}
		if len(r.alerts.Changes) > 0 {
			chchan = r.changes
		}
		if len(r.alerts.Messages) > 0 {
			egress = r.bf.Egress()
		}
	}
}
