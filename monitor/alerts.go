package monitor

import (
	"time"

	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
)

type Config struct {
	Node             *types.Node
	K                int
	LW, HW           int
	TimeoutPeriod    time.Duration
	ReinforceTimeout int
}

func NewAlerts(logger *zap.SugaredLogger, configuration *types.Configuration, conf Config) *Alerts {
	alerts := &Alerts{
		conf:     conf,
		logger:   logger.With("node", conf.Node.ID).Named("alerts"),
		observed: map[uint64]*observedAlert{},
	}
	alerts.Update(configuration)
	return alerts
}

type Alerts struct {
	conf   Config
	logger *zap.SugaredLogger

	instanceID uint64
	kg         *graph.KGraph

	lw, hw int

	observed map[uint64]*observedAlert

	Changes  []*types.Change
	Messages []*types.Message
}

func (a *Alerts) Update(configuration *types.Configuration) {
	a.kg = graph.New(a.conf.K, configuration.Nodes)
	a.instanceID = configuration.ID
	a.lw = a.conf.LW
	if a.lw > a.kg.K {
		a.lw = a.kg.K
	}
	a.hw = a.conf.HW
	if a.hw > a.kg.K {
		a.hw = a.kg.K
	}
	a.logger.With(
		"instanceID", a.instanceID,
		"K", a.kg.K,
		"LW", a.lw,
		"HW", a.hw,
	).Debug("updated alerts reactor")
	for id := range a.observed {
		delete(a.observed, id)
	}
	a.Changes = nil
	a.Messages = nil
}

func (a *Alerts) Tick() {
	for _, observed := range a.observed {
		if len(observed.received) < a.lw {
			continue
		}
		_, voted := observed.received[a.conf.Node.ID]
		if voted {
			continue
		}
		a.kg.IterateObservers(observed.id, func(n *types.Node) bool {
			if n.ID == a.conf.Node.ID {
				observed.ticks++
				return false
			}
			return true
		})
		if observed.ticks == a.conf.ReinforceTimeout {
			alert := types.NewAlert(a.conf.Node.ID, observed.id, observed.change)
			a.logger.With("alert", alert).Debug("reinforce")
			a.send(alert)
			a.Observe(alert)
		}
	}
}

func (a *Alerts) send(msg *types.Message) {
	a.Messages = append(a.Messages, types.WithInstance(a.instanceID, msg))
}

func (a *Alerts) Observe(msg *types.Message) {
	if msg.InstanceID != a.instanceID {
		return
	}
	alert := msg.GetAlert()
	if alert == nil {
		return
	}
	observed := a.observed[alert.Subject]
	if observed == nil {
		observed = &observedAlert{
			change:   alert.Change,
			id:       alert.Subject,
			received: map[uint64]struct{}{},
		}
		a.observed[alert.Subject] = observed
	}

	if observed.detected {
		return
	}

	_, exist := observed.received[alert.Observer]
	if exist {
		return
	}

	a.logger.With(
		"observer", alert.Observer,
		"subject", alert.Subject,
		"change type", alert.Change.Type,
	).Debug("observed first time")
	// record alert from our node in both sets, for sending and retransmission

	observed.received[alert.Observer] = struct{}{}
	count := 0
	a.kg.IterateObservers(alert.Subject, func(n *types.Node) bool {
		if _, exist := observed.received[n.ID]; exist {
			count++
		}
		return true
	})
	a.logger.With(
		"subject", alert.Subject,
		"count", count,
	).Debug("subject count")
	// [lw, hw)
	if count >= a.lw && count < a.hw {
		// check if observers of the unstable subject are unstable themself
		// if they are - count them implicitly
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
		// reverse check. observer detected to be unstable, subjects needs to be updated
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

	// pick list of candidates, but detect them only if all of them are stable
	var candidates []*observedAlert
	for _, other := range a.observed {
		count := 0
		a.kg.IterateObservers(other.id, func(n *types.Node) bool {
			if _, exist := other.received[n.ID]; exist {
				count++
			}
			return true
		})
		// cut is detected only if there are no other unstable alerts
		if count >= a.lw && count < a.hw {
			a.logger.With("subject ID", other.id).Debug("can't detect a cut with unstable subject")
			return
		}
		if count >= a.hw && !other.detected {
			candidates = append(candidates, other)
		}
	}
	for _, cand := range candidates {
		a.logger.With("change", cand.change).Info("detected change")
		cand.detected = true
		a.Changes = append(a.Changes, cand.change)
	}
}

type observedAlert struct {
	id       uint64
	detected bool
	change   *types.Change
	received map[uint64]struct{}
	ticks    int
}
