package monitor

import (
	"time"

	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

type Config struct {
	ID                uint64
	K                 int
	LW, HW            int
	TimeoutPeriod     time.Duration
	ReinforceTimeout  int
	RetransmitTimeout int
}

func NewAlerts(logger *zap.SugaredLogger, kg *KGraph, conf Config) *Alerts {
	alerts := &Alerts{
		conf:       conf,
		logger:     logger,
		id:         conf.ID,
		reinforce:  conf.ReinforceTimeout,
		observed:   map[uint64]*observedAlert{},
		retransmit: conf.RetransmitTimeout,
	}
	alerts.Update(kg)
	return alerts
}

type Alerts struct {
	conf Config

	logger *zap.SugaredLogger

	lw, hw int
	id     uint64

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
	a.lw = a.conf.LW
	if a.lw > a.kg.K {
		a.lw = a.kg.K
	}
	a.hw = a.conf.HW
	if a.hw > a.kg.K {
		a.hw = a.kg.K
	}

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
	// FIXME can be more efficient, no need to loop through same stuff on every iteration

	// record alert from our node in both sets, for sending and retransmission
	if alert.Observer == a.id {
		a.pending = append(a.pending, alert)
		a.alerts = append(a.alerts, alert)
	}
	observed := a.observed[alert.Subject]
	if observed == nil {
		observed = &observedAlert{change: alert.Change, id: alert.Subject, received: map[uint64]struct{}{}}
		a.observed[alert.Subject] = observed
	}
	if observed.detected {
		return
	}

	observed.received[alert.Observer] = struct{}{}
	count := 0
	a.kg.IterateObservers(alert.Subject, func(n *types.Node) bool {
		if _, exist := observed.received[n.ID]; exist {
			count++
		}
		return true
	})

	// [lw, hw)
	if count >= a.lw && count < a.hw {
		// check if observers of the unstable subject are unstable themself
		// if they are count them implicitly
		a.kg.IterateObservers(alert.Subject, func(n *types.Node) bool {
			other := a.observed[n.ID]
			if other == nil {
				return true
			}
			othercnt := 0
			a.kg.IterateObservers(n.ID, func(n *types.Node) bool {
				if _, exist := other.received[n.ID]; exist {
					othercnt++
				}
				return true
			})
			if othercnt >= a.lw {
				observed.received[n.ID] = struct{}{}
			}
			return true
		})
		a.kg.IterateSubjects(alert.Subject, func(n *types.Node) bool {
			subject, exist := a.observed[n.ID]
			if !exist {
				return true
			}
			cnt := 0
			a.kg.IterateObservers(subject.id, func(n *types.Node) bool {
				if _, exist := subject.received[n.ID]; exist {
					cnt++
				}
				return true
			})
			if cnt >= a.lw {
				subject.received[alert.Subject] = struct{}{}
			}
			return true
		})
	}

	// pick list of candidates, but only if there is no unstable alert
	var candidates []*observedAlert
	for _, other := range a.observed {
		othercnt := 0
		a.kg.IterateObservers(other.id, func(n *types.Node) bool {
			if _, exist := other.received[n.ID]; exist {
				othercnt++
			}
			return true
		})
		// cut is detected only if there are no other unstable alerts
		if othercnt >= a.lw && othercnt < a.hw {
			return
		}
		if othercnt >= a.hw && !other.detected {
			candidates = append(candidates, other)
		}
	}
	for _, cand := range candidates {
		cand.detected = true
		a.cuts = append(a.cuts, cand.change)
	}
}

type observedAlert struct {
	id       uint64
	detected bool
	change   *types.Change
	received map[uint64]struct{}
	ticks    int
}
