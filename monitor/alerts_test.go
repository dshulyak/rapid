package monitor_test

import (
	"testing"

	"github.com/dshulyak/rapid/monitor"
	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func genNodes(n int) []*types.Node {
	nodes := make([]*types.Node, n)
	for i := range nodes {
		nodes[i] = &types.Node{
			ID: uint64(i + 1),
		}
	}
	return nodes
}

func TestAlertsCutDetectedFromAllAlerts(t *testing.T) {
	kg := monitor.NewKGraph(100, 3, genNodes(4))
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), kg, monitor.Config{
		ID: 1,
		LW: 3,
		HW: 6,
	})
	change := &types.Change{
		Type: types.Change_REMOVE,
		Node: &types.Node{ID: 2},
	}
	kg.IterateObservers(change.Node.ID, func(n *types.Node) bool {
		alerts.Observe(&mtypes.Alert{
			Observer: n.ID,
			Subject:  change.Node.ID,
			Change:   change,
		})
		return true
	})
	cut := alerts.DetectedCut()
	require.Len(t, cut, 1)
	require.Equal(t, change, cut[0])
}

func TestAlertsUnstableBlocking(t *testing.T) {
	kg := monitor.NewKGraph(100, 8, genNodes(100))
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), kg, monitor.Config{
		ID: 1,
		LW: 1,
		HW: 6,
	})
	changeTWO := &types.Change{
		Type: types.Change_REMOVE,
		Node: &types.Node{ID: 2},
	}
	changeTHREE := &types.Change{
		Type: types.Change_REMOVE,
		Node: &types.Node{ID: 3},
	}
	kg.IterateObservers(changeTHREE.Node.ID, func(n *types.Node) bool {
		alerts.Observe(&mtypes.Alert{
			Observer: n.ID,
			Subject:  changeTHREE.Node.ID,
			Change:   changeTHREE,
		})
		return false
	})
	kg.IterateObservers(changeTWO.Node.ID, func(n *types.Node) bool {
		alerts.Observe(&mtypes.Alert{
			Observer: n.ID,
			Subject:  changeTWO.Node.ID,
			Change:   changeTWO,
		})
		return true
	})
	require.Len(t, alerts.DetectedCut(), 0)
	kg.IterateObservers(changeTHREE.Node.ID, func(n *types.Node) bool {
		alerts.Observe(&mtypes.Alert{
			Observer: n.ID,
			Subject:  changeTHREE.Node.ID,
			Change:   changeTHREE,
		})
		return true
	})
	cut := alerts.DetectedCut()
	require.Len(t, cut, 2)
}

func TestAlertsUnstableObserver(t *testing.T) {
	kg := monitor.NewKGraph(100, 8, genNodes(100))
	hw := 8
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), kg, monitor.Config{
		ID: 1,
		LW: 1,
		HW: hw,
	})
	change := &types.Change{
		Type: types.Change_REMOVE,
		Node: &types.Node{ID: 2},
	}
	var (
		count int
		obsID uint64
	)
	kg.IterateObservers(change.Node.ID, func(n *types.Node) bool {
		if count == hw-1 {
			obsID = n.ID
			return false
		}
		alerts.Observe(&mtypes.Alert{
			Observer: n.ID,
			Subject:  change.Node.ID,
			Change:   change,
		})
		count++
		return true
	})
	obsChange := &types.Change{
		Type: types.Change_REMOVE,
		Node: &types.Node{ID: obsID},
	}
	require.Len(t, alerts.DetectedCut(), 0)
	kg.IterateObservers(obsID, func(n *types.Node) bool {
		alerts.Observe(&mtypes.Alert{
			Observer: n.ID,
			Subject:  obsID,
			Change:   obsChange,
		})
		return true
	})
	require.Len(t, alerts.DetectedCut(), 2)
}

func TestAlertsPendingRecordedAlerts(t *testing.T) {
	kg := monitor.NewKGraph(100, 8, genNodes(100))
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), kg, monitor.Config{
		ID: 1,
		LW: 1,
		HW: 8,
	})
	alert := &mtypes.Alert{
		Observer: 1,
		Subject:  2,
		Change: &types.Change{
			Type: types.Change_REMOVE,
			Node: &types.Node{ID: 2},
		},
	}
	alerts.Observe(alert)
	pending := alerts.Pending()
	require.Len(t, pending, 1)
	require.Equal(t, alert, pending[0])
	require.Len(t, alerts.Pending(), 0)
}

func TestAlertsRetransmit(t *testing.T) {
	kg := monitor.NewKGraph(100, 8, genNodes(100))
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), kg, monitor.Config{
		ID:                1,
		LW:                1,
		HW:                8,
		RetransmitTimeout: 1,
	})
	alert := &mtypes.Alert{
		Observer: 1,
		Subject:  2,
		Change: &types.Change{
			Type: types.Change_REMOVE,
			Node: &types.Node{ID: 2},
		},
	}
	alerts.Observe(alert)
	require.Len(t, alerts.Pending(), 1)
	alerts.Tick()
	pending := alerts.Pending()
	require.Len(t, pending, 1)
	require.Equal(t, alert, pending[0])
}

func TestAlertsReinforce(t *testing.T) {
	kg := monitor.NewKGraph(100, 8, genNodes(100))
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), kg, monitor.Config{
		ID:               1,
		LW:               1,
		HW:               8,
		ReinforceTimeout: 1,
	})
	var subject uint64
	kg.IterateSubjects(1, func(n *types.Node) bool {
		subject = n.ID
		return false
	})
	alert := &mtypes.Alert{
		Observer: 10,
		Subject:  subject,
		Change: &types.Change{
			Type: types.Change_REMOVE,
			Node: &types.Node{ID: subject},
		},
	}
	alerts.Observe(alert)
	alerts.Tick()
	pending := alerts.Pending()
	require.Len(t, pending, 1)
	require.Equal(t, 1, int(pending[0].Observer))
	require.Equal(t, alert.Change, pending[0].Change)
}
