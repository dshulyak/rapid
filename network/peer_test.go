package network

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dshulyak/rapid/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type testDialer struct {
	mock.Mock
}

func (n *testDialer) Dial(ctx context.Context, node *types.Node) (Connection, error) {
	args := n.Called(ctx, node)
	return args.Get(0).(Connection), args.Error(1)
}

type testConn struct {
	mock.Mock
}

func (c *testConn) Send(ctx context.Context, msgs []*types.BroadcastMessage) error {
	args := c.Called(ctx, msgs)
	return args.Error(0)
}

func (c *testConn) Close() error {
	args := c.Called()
	return args.Error(0)
}

type stepTester func(ctx context.Context, state *peerState)

type scenario []stepTester

func (s scenario) test(ctx context.Context, state *peerState) {
	for _, step := range s {
		step(ctx, state)
	}
}

func send(tb testing.TB, p peer, msgs ...[]*types.BroadcastMessage) stepTester {
	return func(ctx context.Context, state *peerState) {
		total := []*types.BroadcastMessage{}
		for _, batch := range msgs {
			require.NoError(tb, p.Send(ctx, batch))
			cont, err := p.selectOne(state, ctx, nil)
			require.NoError(tb, err)
			require.True(tb, cont)
			total = append(total, batch...)
		}
		require.Equal(tb, total, state.Added)
	}
}

func deliver(tb testing.TB, p peer) stepTester {
	return func(ctx context.Context, state *peerState) {
		msgs := state.Added

		cont, err := p.selectOne(state, ctx, nil)
		require.NoError(tb, err)
		require.True(tb, cont)
		require.Nil(tb, state.Requests)
		require.Equal(tb, msgs, state.Pending)
		require.Nil(tb, state.Added)
	}
}

func waitSent(tb testing.TB, p peer) stepTester {
	return func(ctx context.Context, state *peerState) {
		tb.Helper()
		cont, err := p.selectOne(state, ctx, nil)
		require.NoError(tb, err)
		require.True(tb, cont)
		require.Nil(tb, state.Pending, "pending set should be nil")
	}
}

func waitSentErr(tb testing.TB, p peer, err error) stepTester {
	return func(ctx context.Context, state *peerState) {
		tb.Helper()
		msgs := state.Pending
		cont, err := p.selectOne(state, ctx, nil)
		require.Error(tb, err)
		require.True(tb, cont)
		require.Equal(tb, msgs, state.Pending)
		require.EqualError(tb, state.Err, err.Error())
	}
}

func closed(tb testing.TB, p peer, f func()) stepTester {
	return func(ctx context.Context, state *peerState) {
		tb.Helper()
		f()
		cont, err := p.selectOne(state, ctx, nil)
		require.False(tb, cont)
		require.True(tb, errors.Is(err, context.Canceled))
	}
}

func retry(tb testing.TB, p peer) stepTester {
	return func(ctx context.Context, state *peerState) {
		tb.Helper()
		retryc := make(chan time.Time, 1)
		retryc <- time.Time{}
		cont, err := p.selectOne(state, ctx, retryc)
		require.NoError(tb, err)
		require.True(tb, cont)
	}
}

func TestPeerDeliver(t *testing.T) {
	net := &testDialer{}
	p := peer{
		logger: zap.NewNop().Sugar(),
		node:   &types.Node{},
		net:    net,
		egress: make(chan []*types.BroadcastMessage, 1),
	}

	msgs := []*types.BroadcastMessage{{InstanceID: 9}}
	conn := &testConn{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := p.newPeerState(ctx)

	net.On("Dial", mock.Anything, mock.Anything).Return(conn, nil).Once()
	conn.On("Send", mock.Anything, mock.Anything).Return(nil).Once()
	conn.On("Close").Return(nil).Once()

	scenario{
		send(t, p, msgs),
		deliver(t, p),
		waitSent(t, p),
		closed(t, p, cancel),
	}.test(ctx, state)
}

func TestPeerBuffersMessages(t *testing.T) {
	net := &testDialer{}
	p := peer{
		logger: zap.NewNop().Sugar(),
		node:   &types.Node{},
		net:    net,
		egress: make(chan []*types.BroadcastMessage, 1),
	}

	msgs := []*types.BroadcastMessage{{InstanceID: 9}}
	conn := &testConn{}

	net.On("Dial", mock.Anything, mock.Anything).Return(conn, nil).Once()
	waiter := make(chan time.Time)
	conn.On("Send", mock.Anything, mock.Anything).Return(nil).WaitUntil(waiter)
	conn.On("Close").Return(nil).Once()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := p.newPeerState(ctx)

	scenario{
		send(t, p, msgs),
		deliver(t, p),
		send(t, p, msgs, msgs), // response won't be ready, we are blocking on waiter
	}.test(ctx, state)

	close(waiter)

	scenario{
		waitSent(t, p), // first response is ready
		deliver(t, p),  // new request can be accepted
		waitSent(t, p),
		closed(t, p, cancel),
	}.test(ctx, state)
}

func TestPeerBuffersForRetry(t *testing.T) {
	net := &testDialer{}
	p := peer{
		logger: zap.NewNop().Sugar(),
		node:   &types.Node{},
		net:    net,
		egress: make(chan []*types.BroadcastMessage, 1),
	}

	msgs := []*types.BroadcastMessage{{InstanceID: 9}}
	conn := &testConn{}

	terr := errors.New("test error")
	net.On("Dial", mock.Anything, mock.Anything).Return(conn, nil).Once()
	conn.On("Send", mock.Anything, mock.Anything).Return(terr).Once()
	conn.On("Send", mock.Anything, mock.Anything).Return(nil).Once()
	conn.On("Close").Return(nil).Once()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	state := p.newPeerState(ctx)

	scenario{
		send(t, p, msgs),
		deliver(t, p),
		waitSentErr(t, p, terr),
		send(t, p, msgs),
		retry(t, p),
		deliver(t, p),
		waitSent(t, p),
		closed(t, p, cancel),
	}.test(ctx, state)
}
