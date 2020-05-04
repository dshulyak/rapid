package monitor

import (
	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

type Config struct {
	ID                uint64
	LW, HW            int
	ReinforceTimeout  int
	RetransmitTimeout int
}

func NewAlerts(logger *zap.SugaredLogger, kg *KGraph, conf Config) *Alerts {
	return &Alerts{
		logger:     logger,
		id:         conf.ID,
		lw:         conf.LW,
		hw:         conf.HW,
		reinforce:  conf.ReinforceTimeout,
		observed:   map[uint64]*observedAlert{},
		retransmit: conf.RetransmitTimeout,
		kg:         kg,
	}
}

type Alerts struct {
	logger *zap.SugaredLogger

	id     uint64
	lw, hw int // low and high watermark

	// reinforce timeout. alert needs to be reinforced by every observer if it didn't became stable
	// after `reinforce` ticks since first alert was observed.
	reinforce int
	observed  map[uint64]*observedAlert

	// retransmit timeout.
	// alerts are delivered over unreliable broadcast channel, we have no assumption
	// over current connectivity
	retransmit, rticks int
	alerts             []*mtypes.Alert
	pending            []*mtypes.Alert

	cuts []*types.Change

	kg *KGraph
}

func (a *Alerts) DetectedCut() []*types.Change {
	c := a.cuts
	a.cuts = nil
	return c
}

func (a *Alerts) Pending() []*mtypes.Alert {
	rst := a.pending
	a.pending = nil
	if len(rst) > 0 {
		a.rticks = 0
	}
	return rst
}

func (a *Alerts) Update(kg *KGraph) {
	a.kg = kg
	for id := range a.observed {
		delete(a.observed, id)
	}
	a.alerts = nil
	a.pending = nil
	a.cuts = nil
	a.rticks = 0
}

func (a *Alerts) Tick() {
	a.rticks++
	if a.rticks == a.retransmit {
		a.pending = append(a.pending, a.alerts...)
	}
	for _, observed := range a.observed {
		if len(observed.received) < a.lw {
			continue
		}
		_, voted := observed.received[a.id]
		if voted {
			continue
		}
		a.kg.IterateObservers(observed.id, func(n *types.Node) bool {
			if n.ID == a.id {
				observed.ticks++
				return false
			}
			return true
		})
		if observed.ticks == a.reinforce {
			a.Observe(&mtypes.Alert{
				Observer: a.id,
				Subject:  observed.id,
				Change:   observed.change,
			})
		}
	}
}

func (a *Alerts) Observe(alert *mtypes.Alert) {
	if alert.Observer == a.id {
		a.pending = append(a.pending, alert)
		a.alerts = append(a.alerts, alert)
	}
	observed := a.observed[alert.Subject]
	if observed == nil {
		observed = &observedAlert{change: alert.Change, id: alert.Subject}
		a.observed[alert.Subject] = observed
	}
	observed.received[alert.Observer] = struct{}{}
	lth := len(observed.received)
	// [lw, hw)
	if lth >= a.lw && lth < a.hw {
		// check if other observers are unstable
		a.kg.IterateObservers(alert.Subject, func(n *types.Node) bool {
			other := a.observed[n.ID]
			if other == nil {
				return true
			}
			if len(other.received) >= a.lw {
				observed.received[n.ID] = struct{}{}
			}
			return true
		})
	}

	// adjusted after every implicit alert from the unstable proposers
	// [hw, k]
	if len(observed.received) >= a.hw {
		for _, other := range a.observed {
			lth := len(other.received)
			// cut is detected only if there are no other unstable alerts
			if lth >= a.lw && lth < a.hw {
				return
			}
		}
		a.cuts = append(a.cuts, observed.change)
	}
}

type observedAlert struct {
	id       uint64
	change   *types.Change
	received map[uint64]struct{}
	ticks    int
}
