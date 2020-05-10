package consensus_test

import (
	"os"
	"testing"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	atypes "github.com/dshulyak/rapid/types"
)

func nopLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

func devLogger() *zap.SugaredLogger {
	logger, _ := zap.NewDevelopment()
	return logger.Sugar()
}

func testLogger() *zap.SugaredLogger {
	if _, exist := os.LookupEnv("DEBUG_PAXOS"); exist {
		return devLogger()
	}
	return nopLogger()
}

func defaultConfig() consensus.Config {
	return consensus.Config{
		Timeout:          1,
		HeartbeatTimeout: 1,
		Node: &atypes.Node{
			ID: 1,
		},
		Configuration: &atypes.Configuration{
			Nodes: []*atypes.Node{
				{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4},
			},
		},
	}
}
func testPaxos(conf consensus.Config) *consensus.Paxos {
	return consensus.NewPaxos(testLogger(), conf)
}

func TestPaxosPrepareNewOnTimeout(t *testing.T) {
	pax := testPaxos(defaultConfig())

	pax.Tick()
	messages := pax.Messages()
	require.Len(t, messages, 3)

	for i := range messages {
		prepare := messages[i].GetPrepare()
		require.NotNil(t, prepare)
		require.Equal(t, 1, int(prepare.Ballot))
		require.Equal(t, 1, int(prepare.Sequence))
	}
}

func TestPaxosPromiseNilForValidPrepare(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

	msg := types.WithRouting(2, conf.Node.ID, types.NewPrepareMessage(1, 1))

	pax.Step(msg)
	messages := pax.Messages()
	require.Len(t, messages, 1)

	promise := messages[0].GetPromise()
	require.NotNil(t, promise)
	require.Nil(t, promise.Value)
	require.Empty(t, promise.CommitedSequence)
	require.Empty(t, promise.VoteBallot)
}

func TestPaxosAcceptAnyNilPromisesAggregated(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	pax.Tick()
	messages := pax.Messages()
	require.Len(t, messages, 3)

	for i := range messages {
		prepare := messages[i].GetPrepare()
		require.NotNil(t, prepare)
		require.Equal(t, 1, int(prepare.Ballot))
		require.Equal(t, 1, int(prepare.Sequence))
	}

	for _, n := range conf.Configuration.Nodes {
		msg := types.WithRouting(n.ID, 1, types.NewPromiseMessage(1, 1, 0, 0, nil))
		pax.Step(msg)
	}
	messages = pax.Messages()
	require.Len(t, messages, 3)
	for i := range messages {
		accept := messages[i].GetAccept()
		require.True(t, consensus.IsAny(accept.Value))
	}
}

func TestPaxosAcceptedMajority(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	pax.Tick()
	messages := pax.Messages()
	require.Len(t, messages, 3)

	for _, n := range conf.Configuration.Nodes {
		if n.ID != conf.Node.ID {
			pax.Step(types.WithRouting(n.ID, conf.Node.ID, types.NewPromiseMessage(1, 1, 0, 0, nil)))
		}
	}
	messages = pax.Messages()
	require.Len(t, messages, 3)

	id := []byte("replica")
	for _, n := range conf.Configuration.Nodes {
		if n.ID != conf.Node.ID {
			pax.Step(types.WithRouting(n.ID, conf.Node.ID, types.NewAcceptedMessage(1, 1, &types.Value{Id: id})))
		}
	}
	messages = pax.Messages()
	require.Len(t, messages, 6) // 3 Learned + 3 Accept Any

	for i := 3; i < 6; i++ {
		accept := messages[i].GetAccept()
		require.NotNil(t, accept)
		require.True(t, consensus.IsAny(accept.Value))
	}
	values := pax.Values()
	require.Len(t, values, 1)
	require.Equal(t, values[0].Value.Id, id)
}

func TestPaxosAcceptAsHeartbeat(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	pax.Tick()

	for _, n := range conf.Configuration.Nodes {
		if n.ID != conf.Node.ID {
			pax.Step(types.WithRouting(
				n.ID,
				conf.Node.ID,
				types.NewPromiseMessage(1, 1, 0, 0, nil),
			))
		}
	}
	pax.Tick()

	messages := pax.Messages()
	require.Len(t, messages, 9)
	// TODO 3 prepare, 3 accept, 3 heartbeat excluding itself
}
