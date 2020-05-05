package monitor

import (
	"context"
	"time"

	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

func NewAlertsReactor(logger *zap.SugaredLogger, period time.Duration, alerts *Alerts) *AlertsReactor {
	return &AlertsReactor{
		logger:   logger,
		period:   period,
		alerts:   alerts,
		incoming: make(chan *mtypes.Alert, 1),
		changes:  make(chan []*types.Change, 1),
		outgoing: make(chan []*mtypes.Alert, 1),
	}
}

type AlertsReactor struct {
	logger *zap.SugaredLogger
	period time.Duration

	alerts *Alerts

	incoming chan *mtypes.Alert

	changes  chan []*types.Change
	outgoing chan []*mtypes.Alert
}

func (r AlertsReactor) Observe(ctx context.Context, alert *mtypes.Alert) error {
	select {
	case r.incoming <- alert:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (r AlertsReactor) Changes() <-chan []*types.Change {
	return r.changes
}

func (r AlertsReactor) Alerts() <-chan []*mtypes.Alert {
	return r.outgoing
}

func (r AlertsReactor) Run(ctx context.Context) error {
	var (
		ticker   = time.NewTicker(r.period)
		changes  []*types.Change
		outgoing []*mtypes.Alert

		outchan chan []*mtypes.Alert
		chchan  chan []*types.Change
	)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.alerts.Tick()
			outgoing = append(outgoing, r.alerts.Pending()...)
		case <-ctx.Done():
			return ctx.Err()
		case alert := <-r.incoming:
			r.alerts.Observe(alert)
			changes = append(changes, r.alerts.DetectedCut()...)
			outgoing = append(outgoing, r.alerts.Pending()...)
		case chchan <- changes:
			changes = nil
		case outchan <- outgoing:
			outgoing = nil
		}
		if len(changes) > 0 {
			chchan = r.changes
		}
		if len(outgoing) > 0 {
			outchan = r.outgoing
		}
	}
}
