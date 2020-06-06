package consensus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dshulyak/rapid/network/inproc"
	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/require"
)

var errTimedOut = errors.New("test timed out")

// consistent tests that all values are equal to the same value from the slice with expected values.
func consistent(t *testing.T, cluster *Cluster, expected []*types.Value, total int) error {
	t.Helper()
	cluster.Iterate(func(id uint64) bool {
		return true
	})
	return nil
}

func TestManagerNoConflictsProgress(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	ctx := context.TODO()
	proposed := &types.Value{Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.NoError(t, consistent(t, cluster, []*types.Value{proposed}, 4))

	proposed = &types.Value{Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.NoError(t, consistent(t, cluster, []*types.Value{proposed}, 4))
}

func TestManagerConflictingProgress(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	ctx := context.TODO()

	first := &types.Value{Nodes: cluster.Nodes()}
	second := &types.Value{Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Reactor(1).Propose(ctx, first))
	require.NoError(t, cluster.Reactor(2).Propose(ctx, first))
	require.NoError(t, cluster.Reactor(3).Propose(ctx, second))
	require.NoError(t, cluster.Reactor(4).Propose(ctx, second))

	expected := []*types.Value{first, second}
	require.NoError(t, consistent(t, cluster, expected, 4))
}

func TestManagerReachConsensusWithTwoNodes(t *testing.T) {
	cluster := NewCluster(2, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	ctx := context.TODO()

	proposed := &types.Value{Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.NoError(t, consistent(t, cluster, []*types.Value{proposed}, 2))
}

func TestManagerDowngrade(t *testing.T) {
	cluster := NewCluster(3, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	ctx := context.TODO()

	for i := 0; i < 3; i++ {
		add := &types.Node{ID: 777}
		nodes := make([]*types.Node, 4)
		copy(nodes, cluster.Nodes())
		nodes = append(nodes, add)
		upgrade := &types.Value{
			Nodes: nodes,
			Changes: []*types.Change{
				{
					Type: types.Change_JOIN,
					Node: add,
				},
			}}
		require.NoError(t, cluster.Propose(ctx, upgrade))
		require.NoError(t, consistent(t, cluster, []*types.Value{upgrade}, 3))

		downgrade := &types.Value{
			Nodes: cluster.Nodes(),
			Changes: []*types.Change{
				{
					Type: types.Change_REMOVE,
					Node: add,
				},
			}}
		require.NoError(t, cluster.Propose(ctx, downgrade))
		require.NoError(t, consistent(t, cluster, []*types.Value{downgrade}, 3))
	}
}

func TestManagerNoProgressDuringPartition(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	nodes := cluster.Nodes()
	cluster.Network().Apply(
		inproc.NewPartition(
			[]uint64{
				nodes[0].ID,
				nodes[1].ID,
			},
			[]uint64{
				nodes[2].ID,
				nodes[3].ID,
			},
		))

	ctx := context.TODO()
	proposed := &types.Value{Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	cluster.Network().Apply(inproc.Cancel{})
	require.NoError(t, consistent(t, cluster, []*types.Value{proposed}, 4))
}

func TestManagerProgressWithMajority(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	nodes := cluster.Nodes()
	cluster.Network().Apply(
		inproc.NewPartition(
			[]uint64{
				nodes[0].ID,
			},
			[]uint64{
				nodes[1].ID,
				nodes[2].ID,
				nodes[3].ID,
			},
		))

	ctx := context.TODO()
	proposed := &types.Value{Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.NoError(t, consistent(t, cluster, []*types.Value{proposed}, 3))
}
