package consensus_test

import (
	"testing"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func testLogger() *zap.SugaredLogger {
	return zap.NewNop().Sugar()
}

func defaultConfig() consensus.Config {
	return consensus.Config{
		Timeout:          1,
		HeartbeatTimeout: 1,
		ReplicaID:        1,
		ClassicQuorum:    3,
		FastQuorum:       3,
		Replicas:         []uint64{1, 2, 3, 4},
	}
}
func testPaxos(conf consensus.Config) *consensus.Paxos {
	return consensus.NewPaxos(testLogger(), conf)
}

func TestPrepareNewOnTimeout(t *testing.T) {
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

func TestPromiseNilForValidPrepare(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

	msg := types.WithRouting(2, conf.ReplicaID, types.NewPrepareMessage(1, 1))

	pax.Step(msg)
	messages := pax.Messages()
	require.Len(t, messages, 1)

	promise := messages[0].GetPromise()
	require.NotNil(t, promise)
	require.Nil(t, promise.Value)
	require.Empty(t, promise.CommitedSequence)
	require.Empty(t, promise.VoteBallot)
}

func TestAcceptAnyNilPromisesAggregated(t *testing.T) {
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

func TestAcceptedMajority(t *testing.T) {
	conf := defaultConfig()
	pax := testPaxos(conf)

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
	conf := defaultConfig()
	pax := testPaxos(conf)

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
