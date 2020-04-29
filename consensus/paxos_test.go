package consensus_test

import (
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

	pax.Tick()
	messages := pax.Messages()
	require.Len(t, messages, 4)

	for i := range messages {
		prepare := messages[i].GetPrepare()
		require.NotNil(t, prepare)
		require.Equal(t, 1, int(prepare.Ballot))
		require.Equal(t, 1, int(prepare.Sequence))
	}
}

func TestPromiseNilForValidPrepare(t *testing.T) {
	store := memory.New()
	pax := testPaxos(store, defaultConfig())

	msg := types.NewPrepareMessage(1, 1)
	msg.From = 2

	pax.Step(msg)
	messages := pax.Messages()
	require.Len(t, messages, 1)

	promise := messages[0].GetPromise()
	require.NotNil(t, promise)
	require.Nil(t, promise.Value)
	require.Empty(t, promise.CommitedSequence)
	require.Empty(t, promise.VoteBallot)
}

func TestPromisePreviousPromise(t *testing.T) {
	store := memory.New()
	pax := testPaxos(store, defaultConfig())

	msg := types.WithRouting(2, 1, types.NewPrepareMessage(2, 1))

	logs := []*types.LearnedValue{
		{
			Ballot:   1,
			Sequence: 1,
			Value:    &types.Value{Id: []byte("not none")},
		},
	}
	require.NoError(t, store.AddLogs(logs))

	pax.Step(msg)
	messages := pax.Messages()
	require.Len(t, messages, 1)

	promise := messages[0].GetPromise()
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
	pax.Tick()
	messages := pax.Messages()
	require.Len(t, messages, 3)

	for i := range messages {
		prepare := messages[i].GetPrepare()
		require.NotNil(t, prepare)
		require.Equal(t, 1, int(prepare.Ballot))
		require.Equal(t, 1, int(prepare.Sequence))
	}

	for _, r := range conf.Replicas {
		msg := types.WithRouting(r, 1, types.NewPromiseMessage(1, 1, 0, 0, nil))
		pax.Step(msg)
	}
	messages = pax.Messages()
	require.Len(t, messages, 3)
	for i := range messages {
		accept := messages[i].GetAccept()
		require.True(t, consensus.IsAny(accept.Value))
	}
}

func TestAnyAcceptMajorityOfQuorum(t *testing.T) {
	store := memory.New()
	require.NoError(t, store.SetBallot(1))
	conf := defaultConfig()
	pax := testPaxos(store, conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	var (
		first  = []byte("first")
		second = []byte("second")
		send   = []*types.Message{
			types.WithRouting(2, 1, types.NewPromiseMessage(2, 1, 1, 0, &types.Value{Id: second})),
			types.WithRouting(3, 1, types.NewPromiseMessage(2, 1, 1, 0, &types.Value{Id: first})),
		}
	)

	require.NoError(t, store.AddLogs([]*types.LearnedValue{{Ballot: 1, Sequence: 1, Value: &types.Value{Id: first}}}))
	pax.Tick()
	messages := pax.Messages()
	require.Len(t, messages, 3)

	prepare := messages[0].GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, 2, int(prepare.Ballot))
	require.Equal(t, 1, int(prepare.Sequence))

	for _, m := range send {
		pax.Step(m)
	}
	messages = pax.Messages()
	require.Len(t, messages, 3)
	for i := range messages {
		accept := messages[i].GetAccept()
		require.Equal(t, string(first), string(accept.Value.Id))
	}
}

func TestAcceptedMajority(t *testing.T) {
	store := memory.New()
	conf := defaultConfig()
	pax := testPaxos(store, conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	pax.Tick()
	messages := pax.Messages()
	require.Len(t, messages, 3)

	for _, r := range conf.Replicas {
		if r != conf.ReplicaID {
			pax.Step(types.WithRouting(r, conf.ReplicaID, types.NewPromiseMessage(1, 1, 0, 0, nil)))
		}
	}
	messages = pax.Messages()
	require.Len(t, messages, 3)

	id := []byte("replica")
	for _, r := range conf.Replicas {
		if r != conf.ReplicaID {
			pax.Step(types.WithRouting(r, conf.ReplicaID, types.NewAcceptedMessage(1, 1, &types.Value{Id: id})))
		}
	}
	messages = pax.Messages()
	require.Len(t, messages, 6) // 3 Accept Any + 3 Learned

	for i := 0; i < 3; i++ {
		accept := messages[i].GetAccept()
		require.NotNil(t, accept)
		require.True(t, consensus.IsAny(accept.Value))
	}
	values := pax.Values()
	require.Len(t, values, 1)
	require.Equal(t, values[0].Value.Id, id)
}

func TestAcceptAsHeartbeat(t *testing.T) {
	store := memory.New()
	conf := defaultConfig()
	pax := testPaxos(store, conf)

	// Timeout should start a new ballot, and initialize helpers to track received promises
	pax.Tick()

	for _, r := range conf.Replicas {
		if r != conf.ReplicaID {
			pax.Step(types.WithRouting(
				r,
				conf.ReplicaID,
				types.NewPromiseMessage(1, 1, 0, 0, nil),
			))
		}
	}
	pax.Tick()

	messages := pax.Messages()
	require.Len(t, messages, 9)
	// TODO 3 prepare, 3 accept, 3 heartbeat excluding itself
}
