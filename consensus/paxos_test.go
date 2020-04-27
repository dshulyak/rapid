package consensus_test

import (
	"context"
	"testing"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/stores/memory"
	"github.com/dshulyak/rapid/consensus/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err.Error())
	}
	return logger.Sugar()
}

func defaultConfig() consensus.Config {
	return consensus.Config{
		Timeout:          1,
		HeartbeatTimeout: 1,
		ReplicaID:        1,
		FastQuorum:       3,
		Replicas:         []uint64{1, 2, 3, 4},
	}
}
func testPaxos(store consensus.Persistence, conf consensus.Config) *consensus.Paxos {
	return consensus.NewPaxos(testLogger(), store, conf)
}

func TestPrepareNewOnTimeout(t *testing.T) {
	store := memory.New()
	pax := testPaxos(store, defaultConfig())

	require.NoError(t, pax.Tick())
	messages := pax.Messages()
	require.Len(t, messages, 1)

	prepare := messages[0].Message.GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, 1, int(prepare.Ballot))
	require.Equal(t, 1, int(prepare.Sequence))
}

func TestPromiseNilForValidPrepare(t *testing.T) {
	store := memory.New()
	pax := testPaxos(store, defaultConfig())

	msg := consensus.MessageFrom{
		Message: types.NewPrepareMessage(1, 1),
		From:    2,
	}

	require.NoError(t, pax.Step(msg))
	messages := pax.Messages()
	require.Len(t, messages, 1)

	promise := messages[0].Message.GetPromise()
	require.NotNil(t, promise)
	require.Nil(t, promise.Value)
	require.Empty(t, promise.CommitedSequence)
	require.Empty(t, promise.VoteBallot)
}

func TestPromisePreviousPromise(t *testing.T) {
	store := memory.New()
	pax := testPaxos(store, defaultConfig())

	msg := consensus.MessageFrom{
		Message: types.NewPrepareMessage(2, 1),
		From:    2,
	}

	logs := []*types.LearnedValue{
		{
			Ballot:   1,
			Sequence: 1,
			Value:    &types.Value{Id: []byte("not none")},
		},
	}
	require.NoError(t, store.AddLogs(context.TODO(), logs))

	require.NoError(t, pax.Step(msg))
	messages := pax.Messages()
	require.Len(t, messages, 1)

	promise := messages[0].Message.GetPromise()
	require.NotNil(t, promise)
	require.NotNil(t, promise.Value)
	require.Empty(t, promise.CommitedSequence)
	require.Equal(t, logs[0].Ballot, promise.VoteBallot)
}

func TestAcceptAnyNilPromisesAggregated(t *testing.T) {
	store := memory.New()
	conf := defaultConfig()
	pax := testPaxos(store, conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	require.NoError(t, pax.Tick())
	messages := pax.Messages()
	require.Len(t, messages, 1)

	prepare := messages[0].Message.GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, 1, int(prepare.Ballot))
	require.Equal(t, 1, int(prepare.Sequence))

	for _, r := range conf.Replicas {
		require.NoError(t, pax.Step(consensus.MessageFrom{
			From:    r,
			Message: types.NewPromiseMessage(1, 1, 0, 0, nil),
		}))
	}
	replies := pax.Messages()
	require.Len(t, replies, 1)
	accept := replies[0].Message.GetAccept()
	require.True(t, consensus.IsAny(accept.Value))
}

func TestAnyAcceptMajorityOfQuorum(t *testing.T) {
	store := memory.New()
	require.NoError(t, store.SetBallot(context.TODO(), 1))
	conf := defaultConfig()
	pax := testPaxos(store, conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	require.NoError(t, pax.Tick())
	messages := pax.Messages()
	require.Len(t, messages, 1)

	prepare := messages[0].Message.GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, 2, int(prepare.Ballot))
	require.Equal(t, 1, int(prepare.Sequence))

	var (
		first  = []byte("first")
		second = []byte("second")
		send   = []consensus.MessageFrom{
			{
				Message: types.NewPromiseMessage(2, 1, 1, 0, &types.Value{Id: second}),
				From:    2,
			},
			{
				Message: types.NewPromiseMessage(2, 1, 1, 0, &types.Value{Id: first}),
				From:    3,
			},
			{
				Message: types.NewPromiseMessage(2, 1, 1, 0, &types.Value{Id: first}),
				From:    4,
			},
		}
	)
	for _, m := range send {
		require.NoError(t, pax.Step(m))
	}
	replies := pax.Messages()
	require.Len(t, replies, 1)
	accept := replies[0].Message.GetAccept()
	require.Equal(t, string(first), string(accept.Value.Id))
}

func TestAcceptedMajority(t *testing.T) {
	store := memory.New()
	conf := defaultConfig()
	pax := testPaxos(store, conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	require.NoError(t, pax.Tick())
	messages := pax.Messages()
	require.Len(t, messages, 1)

	for _, r := range conf.Replicas {
		require.NoError(t, pax.Step(consensus.MessageFrom{
			From:    r,
			Message: types.NewPromiseMessage(1, 1, 0, 0, nil),
		}))
	}
	messages = pax.Messages()
	require.Len(t, messages, 1)

	id := []byte("replica")
	for _, r := range conf.Replicas {
		require.NoError(t, pax.Step(consensus.MessageFrom{
			From:    r,
			Message: types.NewAcceptedMessage(1, 1, &types.Value{Id: id}),
		}))
	}
	messages = pax.Messages()
	require.Len(t, messages, 4)

	accept := messages[0].Message.GetAccept()
	require.NotNil(t, accept)
	require.True(t, consensus.IsAny(accept.Value))
	values := pax.Values()
	require.Len(t, values, 1)
	require.Equal(t, values[0].Value.Id, id)
}

func TestAcceptAsHeartbeat(t *testing.T) {
	store := memory.New()
	conf := defaultConfig()
	pax := testPaxos(store, conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	require.NoError(t, pax.Tick())

	for _, r := range conf.Replicas {
		require.NoError(t, pax.Step(consensus.MessageFrom{
			From:    r,
			Message: types.NewPromiseMessage(1, 1, 0, 0, nil),
		}))
	}
	require.NoError(t, pax.Tick())

	messages := pax.Messages()
	require.Len(t, messages, 5)
	// TODO check that 1 and 2 are prepare and accept, and 3-5 are accept using accept with nil value
}
