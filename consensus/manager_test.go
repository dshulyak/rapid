package consensus_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/dshulyak/rapid/consensus/types"
	"github.com/stretchr/testify/require"
)

// consistent tests that all values are equal to the same value from the slice with expected values.
func consistent(t *testing.T, values <-chan []*types.LearnedValue, expected []*types.Value, total int) {

	var (
		expectedID []byte
		n          = 0
		after      = time.After(1 * time.Second)
	)
	for learned := range values {
		require.Len(t, learned, 1)
		if expectedID == nil {
			for _, val := range expected {
				if bytes.Compare(val.Id, learned[0].Value.Id) == 0 {
					expectedID = val.Id
				}
			}
		}
		require.Equal(t, expectedID, learned[0].Value.Id)
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

func TestManagerNoConflictsProgress(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	values := make(chan []*types.LearnedValue, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	proposed := &types.Value{Id: []byte("first")}
	require.NoError(t, cluster.Propose(ctx, proposed))
	consistent(t, values, []*types.Value{proposed}, 4)

	proposed = &types.Value{Id: []byte("second")}
	require.NoError(t, cluster.Propose(ctx, proposed))
	consistent(t, values, []*types.Value{proposed}, 4)
}

func TestManagerConflictingProgress(t *testing.T) {
	cluster := NewCluster(4, 10*time.Millisecond, 40)
	cluster.Start()
	defer cluster.Stop()

	values := make(chan []*types.LearnedValue, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.Subscribe(ctx, values)

	first := &types.Value{Id: []byte("first")}
	second := &types.Value{Id: []byte("second")}
	require.NoError(t, cluster.Manager(1).Propose(ctx, first))
	require.NoError(t, cluster.Manager(2).Propose(ctx, first))
	require.NoError(t, cluster.Manager(3).Propose(ctx, second))
	require.NoError(t, cluster.Manager(4).Propose(ctx, second))

	expected := []*types.Value{first, second}
	consistent(t, values, expected, 4)
}
