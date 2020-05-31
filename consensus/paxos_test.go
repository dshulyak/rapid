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
		Timeout: 1,
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

func nConfig(n int) consensus.Config {
	nodes := []*atypes.Node{}
	for i := 1; i <= n; i++ {
		nodes = append(nodes, &atypes.Node{ID: uint64(i)})
	}
	return consensus.Config{
		Timeout: 1,
		Node: &atypes.Node{
			ID: 1,
		},
		Configuration: &atypes.Configuration{
			Nodes: nodes,
		},
	}
}

func testPaxos(conf consensus.Config) *consensus.Paxos {
	return consensus.NewPaxos(testLogger(), conf)
}

func TestPaxosCommitedInFastRound(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

	value := &types.Value{Id: []byte("test")}
	msgs := []*types.Message{
		types.WithRouting(1, conf.Node.ID, types.NewAcceptedMessage(0, 1, value)),
		types.WithRouting(2, conf.Node.ID, types.NewAcceptedMessage(0, 1, value)),
		types.WithRouting(3, conf.Node.ID, types.NewAcceptedMessage(0, 1, value)),
	}
	for _, msg := range msgs {
		pax.Step(msg)
	}

	values := pax.Values()
	require.Len(t, values, 1)
	require.Equal(t, value, values[0].Value)
}

func TestPaxosStartClassicRoundOnTimeout(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

	value := &types.Value{Id: []byte("test")}
	pax.Propose(value)
	msgs := pax.Messages()
	require.Len(t, msgs, 3)
	for _, msg := range msgs {
		accepted := msg.GetAccepted()
		require.NotNil(t, accepted)
		require.Equal(t, value, accepted.Value)
	}
	pax.Tick()
	msgs = pax.Messages()
	require.Len(t, msgs, 3)
	for _, msg := range msgs {
		prepare := msg.GetPrepare()
		require.NotNil(t, prepare)
	}
}

func TestPaxosFullRoundWithConflicts(t *testing.T) {
	n := 8
	conf := nConfig(n)
	pax := testPaxos(conf)

	one := &types.Value{Id: []byte("one")}
	two := &types.Value{Id: []byte("two")}

	pax.Propose(one)
	require.Len(t, pax.Messages(), n-1)
	for _, node := range conf.Configuration.Nodes[1:5] {
		pax.Step(types.WithRouting(node.ID, conf.Node.ID, types.NewAcceptedMessage(0, 1, one)))
	}
	for _, node := range conf.Configuration.Nodes[5:n] {
		pax.Step(types.WithRouting(node.ID, conf.Node.ID, types.NewAcceptedMessage(0, 1, two)))
	}
	require.Empty(t, pax.Messages())
	require.Empty(t, pax.Values())

	pax.Tick()
	msgs := pax.Messages()
	require.Len(t, msgs, n-1)

	for _, msg := range msgs {
		prepare := msg.GetPrepare()
		require.NotNil(t, prepare)
		require.Equal(t, 1, int(prepare.Ballot))
	}

	for _, node := range conf.Configuration.Nodes[1:5] {
		pax.Step(types.WithRouting(node.ID, conf.Node.ID, types.NewPromiseMessage(1, 1, 1, one)))
	}
	for _, node := range conf.Configuration.Nodes[5:n] {
		pax.Step(types.WithRouting(node.ID, conf.Node.ID, types.NewPromiseMessage(1, 1, 1, two)))
	}

	msgs = pax.Messages()
	require.Len(t, msgs, 2*n-2)
	for _, msg := range msgs {
		accept := msg.GetAccept()
		if accept != nil {
			require.Equal(t, one, accept.Value)
		} else {
			accepted := msg.GetAccepted()
			require.NotNil(t, accepted)
			require.Equal(t, one, accepted.Value)
		}
	}
}
