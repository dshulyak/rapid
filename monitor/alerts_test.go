package monitor_test

import (
	"testing"

	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func genConfiguration(n int) *types.Configuration {
	nodes := make([]*types.Node, n)
	for i := range nodes {
		nodes[i] = &types.Node{
			ID: uint64(i + 1),
		}
	}
	return &types.Configuration{Nodes: nodes}
}

func TestAlertsCutDetectedFromAllAlerts(t *testing.T) {
	conf := genConfiguration(4)
	kg := graph.New(3, conf.Nodes)
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(),
		conf,
		monitor.Config{
			Node: &types.Node{ID: 1},
			K:    3,
			LW:   3,
			HW:   6,
		})
	change := &types.Change{
		Type: types.Change_REMOVE,
		Node: &types.Node{ID: 2},
	}
	kg.IterateObservers(change.Node.ID, func(n *types.Node) bool {
		alerts.Observe(types.NewAlert(n.ID, change.Node.ID, change))
		return true
	})
	cut := alerts.Changes
	require.Len(t, cut, 1)
	require.Equal(t, change, cut[0])
}

func TestAlertsUnstableBlocking(t *testing.T) {
	conf := genConfiguration(100)
	kg := graph.New(8, conf.Nodes)
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(),
		conf, monitor.Config{
			Node: &types.Node{ID: 1},
			K:    8,
			LW:   1,
			HW:   6,
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
		alerts.Observe(types.NewAlert(n.ID, changeTHREE.Node.ID, changeTHREE))
		return false
	})
	kg.IterateObservers(changeTWO.Node.ID, func(n *types.Node) bool {
		alerts.Observe(types.NewAlert(n.ID, changeTWO.Node.ID, changeTWO))
		return true
	})
	require.Len(t, alerts.Changes, 0)
	kg.IterateObservers(changeTHREE.Node.ID, func(n *types.Node) bool {
		alerts.Observe(types.NewAlert(n.ID, changeTHREE.Node.ID, changeTHREE))
		return true
	})
	cut := alerts.Changes
	require.Len(t, cut, 2)
}

func TestAlertsUnstableObserver(t *testing.T) {
	hw := 8
	conf := genConfiguration(100)
	kg := graph.New(8, conf.Nodes)
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), conf,
		monitor.Config{
			Node: &types.Node{ID: 1},
			K:    8,
			LW:   1,
			HW:   hw,
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
		alerts.Observe(types.NewAlert(n.ID, change.Node.ID, change))
		count++
		return true
	})
	obsChange := &types.Change{
		Type: types.Change_REMOVE,
		Node: &types.Node{ID: obsID},
	}
	require.Len(t, alerts.Changes, 0)
	kg.IterateObservers(obsID, func(n *types.Node) bool {
		alerts.Observe(types.NewAlert(n.ID, obsID, obsChange))
		return true
	})
	require.Len(t, alerts.Changes, 2)
}

func TestAlertsReinforce(t *testing.T) {
	conf := genConfiguration(100)
	kg := graph.New(8, conf.Nodes)
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), conf, monitor.Config{
		Node:             &types.Node{ID: 1},
		K:                8,
		LW:               1,
		HW:               8,
		ReinforceTimeout: 1,
	})
	var subject uint64
	kg.IterateSubjects(1, func(n *types.Node) bool {
		subject = n.ID
		return false
	})
	change := &types.Change{
		Type: types.Change_REMOVE,
		Node: &types.Node{ID: subject},
	}
	msg := types.NewAlert(10, subject, change)

	alerts.Observe(msg)
	alerts.Tick()

	require.Len(t, alerts.Messages, 1)
	reinforced := alerts.Messages[0].GetAlert()
	require.Equal(t, 1, int(reinforced.Observer))
	require.Equal(t, change, reinforced.Change)
}

func TestJoinedNonexisting(t *testing.T) {
	conf := genConfiguration(100)
	kg := graph.New(8, conf.Nodes)
	alerts := monitor.NewAlerts(zap.NewNop().Sugar(), conf, monitor.Config{
		Node: &types.Node{ID: 1},
		K:    8,
		LW:   1,
		HW:   8,
	})

	joiner := uint64(1000)
	kg.IterateObservers(joiner, func(n *types.Node) bool {
		alerts.Observe(types.NewAlert(n.ID, joiner, &types.Change{
			Type: types.Change_JOIN,
			Node: &types.Node{ID: joiner},
		}))
		return true
	})

	changes := alerts.Changes
	require.Len(t, changes, 1)
}
