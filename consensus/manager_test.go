package consensus_test

import (
	"context"
	"testing"
	"time"

	"github.com/dshulyak/rapid/consensus/types"
	"github.com/stretchr/testify/require"
)

func consistent(t *testing.T, values <-chan []*types.LearnedValue, expected *types.Value, total int) {
	n := 0
	after := time.After(1 * time.Second)
	for learned := range values {
		require.Len(t, learned, 1)
		require.Equal(t, expected.Id, learned[0].Value.Id)
		select {
		case <-after:
			require.FailNow(t, "not enough learned values")
		default:
			n++
			if n == total {
				return
			}
		}
	}
}

func TestManagerSimpleProgress(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	values := make(chan []*types.LearnedValue, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	proposed := &types.Value{Id: []byte("first")}
	require.NoError(t, cluster.Propose(ctx, proposed))
	consistent(t, values, proposed, 4)

	proposed = &types.Value{Id: []byte("second")}
	require.NoError(t, cluster.Propose(ctx, proposed))
	consistent(t, values, proposed, 4)
}
