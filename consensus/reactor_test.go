package consensus_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/dshulyak/rapid/network/inproc"
	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/require"
	"gotest.tools/assert"
)

var errTimedOut = errors.New("test timed out")

func verifyUpdated(t *testing.T,
	cluster *Cluster,
	mutator func(),
	nodes []*types.Node,
	timeout time.Duration,
	proposals ...[]*types.Node,
) {
	t.Helper()
	updates := map[uint64]<-chan struct{}{}
	for _, n := range nodes {
		_, update := cluster.Last(n.ID).Last()
		updates[n.ID] = update
	}
	mutator()
	after := time.After(timeout)
	for _, update := range updates {
		select {
		case <-update:
		case <-after:
			require.FailNow(t, "failed waiting for updated configuration")
		}
	}
	for _, proposal := range proposals {
		sort.Slice(proposal, func(i, j int) bool {
			return proposal[i].ID < proposal[j].ID
		})
		for _, n := range nodes {
			configuration := cluster.Last(n.ID).Configuration()
			sort.Slice(configuration.Nodes, func(i, j int) bool {
				return configuration.Nodes[i].ID < configuration.Nodes[j].ID
			})
			for i := range proposal {
				assert.Equal(t, proposal[i], configuration.Nodes[i])
			}
		}
	}
}

func TestReactorProposeSameValue(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	ctx := context.TODO()

	added := &types.Node{ID: 777}
	changes := []*types.Change{
		{
			Type: types.Change_JOIN,
			Node: added,
		},
	}
	for i := 0; i < 2; i++ {
		added.ID++
		verifyUpdated(t, cluster, func() {
			require.NoError(t, cluster.Propose(ctx, &types.Value{Changes: changes}))
		}, cluster.Nodes(), 10*time.Second, append(cluster.Nodes(), added))
	}
}

func TestReactorReachConsensusWithTwoNodes(t *testing.T) {
	cluster := NewCluster(2, 40*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	ctx := context.TODO()

	added := &types.Node{ID: 777}
	changes := []*types.Change{
		{
			Type: types.Change_JOIN,
			Node: added,
		},
	}
	proposed := &types.Value{
		Changes: changes,
	}
	verifyUpdated(t, cluster, func() {
		require.NoError(t, cluster.Propose(ctx, proposed))
	}, cluster.Nodes(), 10*time.Second, append(cluster.Nodes(), added))
}

func TestReactorDowngrade(t *testing.T) {
	cluster := NewCluster(3, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	ctx := context.TODO()

	node := &types.Node{ID: 777}
	for i := 0; i < 3; i++ {
		nodes := make([]*types.Node, 3)
		copy(nodes, cluster.Nodes())
		nodes = append(nodes, node)
		upgrade := &types.Value{
			Changes: []*types.Change{
				{
					Type: types.Change_JOIN,
					Node: node,
				},
			},
		}

		verifyUpdated(t, cluster, func() {
			require.NoError(t, cluster.Propose(ctx, upgrade))
		}, cluster.Nodes(), 10*time.Second, nodes)

		downgrade := &types.Value{
			Changes: []*types.Change{
				{
					Type: types.Change_REMOVE,
					Node: node,
				},
			}}

		verifyUpdated(t, cluster, func() {
			require.NoError(t, cluster.Propose(ctx, downgrade))
		}, cluster.Nodes(), 10*time.Second, cluster.Nodes())
	}
}

func TestReactorProgressWithMajority(t *testing.T) {
	size := 5
	cluster := NewCluster(size, 20*time.Millisecond, 60)
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
				nodes[4].ID,
			},
		))

	ctx := context.TODO()

	node := &types.Node{ID: 777}
	upgrade := &types.Value{
		Changes: []*types.Change{
			{
				Type: types.Change_JOIN,
				Node: node,
			},
		},
	}
	verifyUpdated(t, cluster, func() {
		require.NoError(t, cluster.Propose(ctx, upgrade))
	}, nodes[2:], 10*time.Second, cluster.Nodes())
}
