package consensus_test

import (
	"os"
	"testing"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
		Node: &types.Node{
			ID: 1,
		},
		Configuration: &types.Configuration{
			Nodes: []*types.Node{
				{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4},
			},
		},
	}
}

func nConfig(n int) consensus.Config {
	nodes := []*types.Node{}
	for i := 1; i <= n; i++ {
		nodes = append(nodes, &types.Node{ID: uint64(i)})
	}
	return consensus.Config{
		Timeout: 1,
		Node: &types.Node{
			ID: 1,
		},
		Configuration: &types.Configuration{
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

	value := &types.Value{}
	msgs := []*types.Message{
		types.WithRouting(1, nil, types.NewAcceptedMessage(0, 1, value)),
		types.WithRouting(2, nil, types.NewAcceptedMessage(0, 1, value)),
		types.WithRouting(3, nil, types.NewAcceptedMessage(0, 1, value)),
	}
	for _, msg := range msgs {
		pax.Step(msg)
	}
	require.NotNil(t, pax.Update)
}

func TestPaxosStartClassicRoundOnTimeout(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

	value := &types.Value{}
	pax.Propose(value)
	pax.Tick()
	require.Len(t, pax.Messages, 2)
	msgs := pax.Messages
	accepted := msgs[0].GetAccepted()
	require.NotNil(t, accepted)
	require.Equal(t, value, accepted.Value)

	prepare := msgs[1].GetPrepare()
	require.NotNil(t, prepare)
}

func TestPaxosFullRoundWithConflicts(t *testing.T) {
	n := 7
	conf := nConfig(n)
	pax := testPaxos(conf)

	one := &types.Value{Changes: []*types.Change{
		{
			Type: types.Change_JOIN,
			Node: &types.Node{},
		}}}
	two := &types.Value{Changes: []*types.Change{
		{
			Type: types.Change_REMOVE,
			Node: &types.Node{},
		}}}

	pax.Propose(one)
	require.Len(t, pax.Messages, 1)
	pax.Messages = nil
	for _, node := range conf.Configuration.Nodes[1:4] {
		pax.Step(types.WithRouting(node.ID, nil, types.NewAcceptedMessage(0, 1, one)))
	}
	for _, node := range conf.Configuration.Nodes[4:n] {
		pax.Step(types.WithRouting(node.ID, nil, types.NewAcceptedMessage(0, 1, two)))
	}
	require.Empty(t, pax.Messages)
	require.Empty(t, pax.Update)

	pax.Tick()
	msgs := pax.Messages
	require.Len(t, msgs, 1)

	prepare := msgs[0].GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, 1, int(prepare.Ballot))
	pax.Messages = nil

	for _, node := range conf.Configuration.Nodes[1:5] {
		pax.Step(types.WithRouting(node.ID, nil, types.NewPromiseMessage(1, 1, 1, one)))
	}
	for _, node := range conf.Configuration.Nodes[5:n] {
		pax.Step(types.WithRouting(node.ID, nil, types.NewPromiseMessage(1, 1, 1, two)))
	}

	msgs = pax.Messages
	require.Len(t, msgs, 2)

	accept := msgs[0].GetAccept()
	require.Equal(t, one, accept.Value)

	accepted := msgs[1].GetAccepted()
	require.NotNil(t, accepted)
	require.Equal(t, one, accepted.Value)
}
