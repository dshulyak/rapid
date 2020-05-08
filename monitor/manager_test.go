package monitor_test

import (
	"context"
	"testing"
	"time"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/monitor/network/inproc"
	network "github.com/dshulyak/rapid/network/inproc"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type NopFailureDetector struct{}

func (fd NopFailureDetector) Monitor(ctx context.Context, _ *types.Node) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestManagerJoin(t *testing.T) {
	nodes := genNodes(20)
	net := network.NewNetwork()
	defer net.Stop()

	logger := zap.NewNop().Sugar()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	managers := []monitor.Manager{}

	template := monitor.Config{
		K:                 3,
		LW:                2,
		HW:                3,
		TimeoutPeriod:     100 * time.Millisecond,
		ReinforceTimeout:  3,
		RetransmitTimeout: 10,
	}
	kg := monitor.NewKGraph(template.K, nodes)

	for _, n := range nodes {
		conf := template
		conf.ID = n.ID

		man := monitor.NewManager(
			logger,
			conf,
			kg,
			NopFailureDetector{},
			inproc.New(logger, n.ID, net),
		)
		managers = append(managers, man)
		group.Go(func() error {
			return man.Run(ctx)
		})
	}

	group.Wait()
}
