package monitor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/monitor/network/inproc"
	network "github.com/dshulyak/rapid/network/inproc"
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
	network  *network.Network
	logger   *zap.SugaredLogger
	template monitor.Config

	fd monitor.FailureDetector

	managers []monitor.Manager

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
	for _, n := range tc.initial.Nodes {
		conf := tc.template
		conf.ID = n.ID
		conf.Node = n
		man := monitor.NewManager(
			tc.logger,
			conf,
			tc.initial,
			tc.fd,
			inproc.New(tc.logger, n.ID, tc.network),
		)
		tc.managers = append(tc.managers, man)
		group.Go(func() error {
			return man.Run(ctx)
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

func TestManagerJoin(t *testing.T) {
	nodes := genNodes(20)
	net := network.NewNetwork()
	defer net.Stop()

	tc := testCluster{
		logger:  zap.NewNop().Sugar(),
		initial: &types.Configuration{Nodes: nodes},
		fd:      TestFailureDetector{},
		network: net,
		template: monitor.Config{
			K:                 3,
			LW:                2,
			HW:                3,
			TimeoutPeriod:     100 * time.Millisecond,
			ReinforceTimeout:  3,
			RetransmitTimeout: 10,
		},
	}
	tc.setup()
	defer tc.stop()

	joiner := &types.Node{ID: 101}
	conf := tc.template
	conf.Node = joiner
	conf.ID = joiner.ID

	man := monitor.NewManager(
		tc.logger,
		conf,
		tc.initial,
		tc.fd,
		inproc.New(tc.logger, joiner.ID, net),
	)
	require.NoError(t, man.Join(context.TODO()))

	cuts := [][]*types.Change{}
	for _, m := range tc.managers {
		select {
		case cut := <-m.Changes():
			cuts = append(cuts, cut)
		case <-time.After(time.Second):
			require.FailNow(t, "timed out waitign for changes")
		}
	}
	require.Len(t, cuts, len(tc.managers))
}

func TestManagerDetectFailed(t *testing.T) {
	nodes := genNodes(20)
	net := network.NewNetwork()
	defer net.Stop()

	failed := TestFailureDetector{1: struct{}{}}
	tc := testCluster{
		logger:  zap.NewNop().Sugar(),
		initial: &types.Configuration{Nodes: nodes},
		fd:      failed,
		network: net,
		template: monitor.Config{
			K:                 3,
			LW:                2,
			HW:                3,
			TimeoutPeriod:     10 * time.Millisecond,
			ReinforceTimeout:  3,
			RetransmitTimeout: 10,
		},
	}
	tc.setup()
	defer tc.stop()

	cuts := [][]*types.Change{}
	for _, m := range tc.managers {
		select {
		case cut := <-m.Changes():
			cuts = append(cuts, cut)
		case <-time.After(time.Second):
			require.FailNow(t, "timed out waitign for changes")
		}
	}
	require.Len(t, cuts, len(tc.managers))
	for _, cut := range cuts {
		require.Len(t, cut, len(failed))
		for _, change := range cut {
			require.Contains(t, failed, change.Node.ID)
		}
	}
}
