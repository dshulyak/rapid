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
		Timeout:   1,
		ReplicaID: 1,
		Quorum:    3,
		Replicas:  []uint64{1, 2, 3, 4},
	}
}
func testPaxos(store consensus.Persistence, conf consensus.Config) *consensus.Paxos {
	return consensus.NewPaxos(testLogger(), store, conf)
}

func TestPrepareNewOnTimeout(t *testing.T) {
	store := memory.New()
	pax := testPaxos(store, defaultConfig())

	messages, err := pax.Tick()
	require.NoError(t, err)
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

	messages, err := pax.Step(msg)
	require.NoError(t, err)
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

	messages, err := pax.Step(msg)
	require.NoError(t, err)
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
	messages, err := pax.Tick()
	require.NoError(t, err)
	require.Len(t, messages, 1)

	prepare := messages[0].Message.GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, 1, int(prepare.Ballot))
	require.Equal(t, 1, int(prepare.Sequence))

	var replies []consensus.MessageTo
	for _, r := range conf.Replicas {
		messages, err := pax.Step(consensus.MessageFrom{
			From:    r,
			Message: types.NewPromiseMessage(1, 1, 0, 0, nil),
		})
		require.NoError(t, err)
		replies = append(replies, messages...)
	}
	require.Len(t, replies, 1)
	accept := replies[0].Message.GetAccept()
	require.True(t, consensus.IsAny(accept.Value))
}

func TestAnyAcceptMajorityOfQuorum(t *testing.T) {
	store := memory.New()
	store.SetBallot(context.TODO(), 1)
	conf := defaultConfig()
	pax := testPaxos(store, conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	messages, err := pax.Tick()
	require.NoError(t, err)
	require.Len(t, messages, 1)

	prepare := messages[0].Message.GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, 2, int(prepare.Ballot))
	require.Equal(t, 1, int(prepare.Sequence))

	var (
		first   = []byte("first")
		second  = []byte("second")
		replies []consensus.MessageTo
		send    = []consensus.MessageFrom{
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
		step, err := pax.Step(m)
		require.NoError(t, err)
		replies = append(replies, step...)
	}
	require.Len(t, replies, 1)
	accept := replies[0].Message.GetAccept()
	require.Equal(t, string(first), string(accept.Value.Id))
}
