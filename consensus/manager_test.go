package consensus_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	ctypes "github.com/dshulyak/rapid/consensus/types"
	"github.com/dshulyak/rapid/network/inproc"
	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/require"
)

var errTimedOut = errors.New("test timed out")

// consistent tests that all values are equal to the same value from the slice with expected values.
func consistent(t *testing.T, values <-chan []*ctypes.LearnedValue, expected []*ctypes.Value, total int) error {
	var (
		expectedID []byte
		n          = 0
		after      = time.After(1 * time.Second)
	)
	for {
		select {
		case <-after:
			return errTimedOut
		case learned := <-values:
			require.Len(t, learned, 1)
			if expectedID == nil {
				for _, val := range expected {
					if bytes.Compare(val.Id, learned[0].Value.Id) == 0 {
						expectedID = val.Id
					}
				}
			}
			require.Equal(t, expectedID, learned[0].Value.Id)

			n++
			if n == total {
				return nil
			}
		}
	}
}

func TestManagerNoConflictsProgress(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	values := make(chan []*ctypes.LearnedValue, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	proposed := &ctypes.Value{Id: []byte("first"), Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.NoError(t, consistent(t, values, []*ctypes.Value{proposed}, 4))

	proposed = &ctypes.Value{Id: []byte("second"), Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.NoError(t, consistent(t, values, []*ctypes.Value{proposed}, 4))
}

func TestManagerConflictingProgress(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	values := make(chan []*ctypes.LearnedValue, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	first := &ctypes.Value{Id: []byte("first"), Nodes: cluster.Nodes()}
	second := &ctypes.Value{Id: []byte("second"), Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Manager(1).Propose(ctx, first))
	require.NoError(t, cluster.Manager(2).Propose(ctx, first))
	require.NoError(t, cluster.Manager(3).Propose(ctx, second))
	require.NoError(t, cluster.Manager(4).Propose(ctx, second))

	expected := []*ctypes.Value{first, second}
	require.NoError(t, consistent(t, values, expected, 4))
}

func TestManagerReachConsensusWithTwoNodes(t *testing.T) {
	cluster := NewCluster(2, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	values := make(chan []*ctypes.LearnedValue, 2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	proposed := &ctypes.Value{Id: []byte("first"), Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.NoError(t, consistent(t, values, []*ctypes.Value{proposed}, 2))
}

func TestManagerDowngrade(t *testing.T) {
	cluster := NewCluster(3, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	values := make(chan []*ctypes.LearnedValue, 3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	for i := 0; i < 3; i++ {
		add := &types.Node{ID: 777}
		nodes := make([]*types.Node, 0, 4)
		nodes = append(nodes, cluster.Nodes()...)
		nodes = append(nodes, add)
		upgrade := &ctypes.Value{
			Id:    []byte("upgrade"),
			Nodes: nodes,
			Changes: &types.Changes{List: []*types.Change{
				{
					Type: types.Change_JOIN,
					Node: add,
				},
			}}}
		require.NoError(t, cluster.Propose(ctx, upgrade))
		require.NoError(t, consistent(t, values, []*ctypes.Value{upgrade}, 3))

		downgrade := &ctypes.Value{
			Id:    []byte("downgrade"),
			Nodes: cluster.Nodes(),
			Changes: &types.Changes{List: []*types.Change{
				{
					Type: types.Change_REMOVE,
					Node: add,
				},
			}}}
		require.NoError(t, cluster.Propose(ctx, downgrade))
		require.NoError(t, consistent(t, values, []*ctypes.Value{downgrade}, 3))
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

	values := make(chan []*ctypes.LearnedValue, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	proposed := &ctypes.Value{Id: []byte("first"), Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.Error(t, consistent(t, values, []*ctypes.Value{proposed}, 4))

	cluster.Network().Apply(inproc.Cancel{})
	require.NoError(t, consistent(t, values, []*ctypes.Value{proposed}, 4))
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

	values := make(chan []*ctypes.LearnedValue, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	proposed := &ctypes.Value{Id: []byte("first"), Nodes: cluster.Nodes()}
	require.NoError(t, cluster.Propose(ctx, proposed))
	require.NoError(t, consistent(t, values, []*ctypes.Value{proposed}, 3))
}
