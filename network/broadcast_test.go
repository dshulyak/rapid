package network_test

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/network/inproc"
	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func nopLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

func devLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func testLogger() *zap.SugaredLogger {
	if _, exist := os.LookupEnv("DEBUG_BROADCAST"); exist {
		return devLogger()
	}
	return nopLogger()
}

func TestBroadcasterDeliver(t *testing.T) {
	net := inproc.NewNetwork()
	size := 5

	nodes := make([]*types.Node, size)
	for i := 1; i <= size; i++ {
		nodes[i-1] = &types.Node{ID: uint64(i)}
	}
	last := types.Last(&types.Configuration{Nodes: nodes})

	broadcaster := make([]network.ReliableBroadcast, size)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)
	for i := range nodes {
		i := i
		broadcaster[i] = network.NewReliableBroadcast(
			testLogger().With("node", nodes[i].ID),
			net.BroadcastNetwork(nodes[i].ID),
			last,
			network.Config{
				NodeID:      nodes[i].ID,
				Fanout:      1,
				DialTimeout: 3 * time.Second,
				SendTimeout: 3 * time.Second,
				RetryPeriod: 3 * time.Second,
			},
		)
		group.Go(func() error {
			return broadcaster[i].Run(ctx)
		})
	}
	rand.Seed(time.Now().Unix())
	for i := 0; i < 7; i++ {
		sender := rand.Intn(len(broadcaster))
		sent := []*types.Message{{
			From:       uint64(sender + 1),
			InstanceID: uint64(i),
		}}
		broadcaster[sender].Egress() <- sent

		for i := 0; i < len(broadcaster); i++ {
			if i == sender {
				continue
			}
			select {
			case msgs := <-broadcaster[i].Watch():
				require.Len(t, msgs, len(sent))
				for i := range sent {
					require.Equal(t, sent[i], msgs[i])
				}
			case <-time.After(time.Second):
				require.FailNow(t, "failed waiting for messages")
			}
		}
	}
}
