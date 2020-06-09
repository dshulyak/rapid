package monitor_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/network/inproc"
	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type TestFailureDetector map[uint64]struct{}

func (fd TestFailureDetector) Monitor(ctx context.Context, n *types.Node) error {
	if _, exist := fd[n.ID]; exist {
		return errors.New("failed")
	}
	<-ctx.Done()
	return ctx.Err()
}

type testCluster struct {
	initial  *types.Configuration
	network  *inproc.Network
	logger   *zap.SugaredLogger
	template monitor.Config

	fd monitor.FailureDetector

	managers map[uint64]*monitor.Manager

	group  *errgroup.Group
	ctx    context.Context
	cancel func()
}

func (tc *testCluster) setup() {
	ctx, cancel := context.WithCancel(context.Background())
	tc.ctx = ctx
	tc.cancel = cancel
	group, ctx := errgroup.WithContext(ctx)
	tc.group = group
	tc.managers = map[uint64]*monitor.Manager{}
	last := types.Last(tc.initial)
	for _, n := range tc.initial.Nodes {
		conf := tc.template
		conf.Node = n
		bf := network.NewBroadcastFacade(
			network.NewReliableBroadcast(
				tc.logger,
				tc.network.BroadcastNetwork(n.ID),
				last,
				network.Config{
					NodeID:      n.ID,
					Fanout:      1,
					DialTimeout: time.Second,
					SendTimeout: time.Second,
					RetryPeriod: 10 * time.Second,
				}))
		man := monitor.NewManager(
			tc.logger,
			conf,
			last,
			tc.fd,
			bf,
		)
		tc.managers[n.ID] = man
		group.Go(func() error {
			return man.Run(ctx)
		})
		group.Go(func() error {
			return bf.Run(ctx)
		})
	}
}

func (tc *testCluster) stop() {
	if tc.cancel != nil {
		tc.cancel()
		tc.cancel = nil
	}
	if tc.group != nil {
		tc.group.Wait()
		tc.group = nil
	}
}

func nopLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

func devLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func testLogger() *zap.SugaredLogger {
	if _, exist := os.LookupEnv("DEBUG_MONITOR"); exist {
		return devLogger()
	}
	return nopLogger()
}

func TestManagerDetectFailed(t *testing.T) {
	configuration := genConfiguration(4)
	net := inproc.NewNetwork()
	defer net.Stop()

	failed := TestFailureDetector{1: struct{}{}}
	tc := testCluster{
		logger:  testLogger(),
		initial: configuration,
		fd:      failed,
		network: net,
		template: monitor.Config{
			K:                3,
			LW:               2,
			HW:               3,
			TimeoutPeriod:    10 * time.Millisecond,
			ReinforceTimeout: 3,
		},
	}
	tc.setup()
	defer tc.stop()

	cuts := [][]*types.Change{}
	for id, m := range tc.managers {
		if _, exist := failed[id]; exist {
			continue
		}
		select {
		case cut := <-m.Changes():
			cuts = append(cuts, cut)
		case <-time.After(3 * time.Second):
			require.FailNow(t, "timed out waitign for changes")
		}
	}
	require.Len(t, cuts, len(tc.managers)-1)
	for _, cut := range cuts {
		require.Len(t, cut, len(failed))
		for _, change := range cut {
			require.Contains(t, failed, change.Node.ID)
		}
	}
}
