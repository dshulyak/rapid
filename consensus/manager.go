package consensus

import (
	"context"
	"errors"
	"sync"

	"github.com/dshulyak/rapid/consensus/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrNetwork raised for any issue related to network IO while sending/receiving messages.
	ErrNetwork = errors.New("network is unreliable")
)

type ConsumeFn func(context.Context, *types.Message) error

type Swarm interface {
	Send(context.Context, *types.Message) error
	Consume(context.Context, ConsumeFn) error
}

func NewManager(logger *zap.SugaredLogger, cons *Consensus, swarm Swarm) *Manager {
	return &Manager{
		logger:    logger.Named("manager"),
		consensus: cons,
		swarm:     swarm,
		bus:       newBus(),
	}
}

// Manager stitches all subcomponents together (swarm, consensus) and provides simplified API
// to consume consensus subsystem.
type Manager struct {
	logger    *zap.SugaredLogger
	consensus *Consensus
	swarm     Swarm

	bus *valuesBus
}

func (m *Manager) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return m.consensus.Run(ctx)
	})
	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msgs := <-m.consensus.Messages():
				// TODO each message should be sent to separate channel to prevent blocking this node.
				for i := range msgs {
					if err := m.swarm.Send(ctx, msgs[i]); err != nil && !errors.Is(err, context.Canceled) {
						m.logger.Warn("failed to send=", err)
					}
				}
			}
		}
	})
	group.Go(func() error {
		return m.swarm.Consume(ctx, func(_ context.Context, msg *types.Message) error {
			// if context is nil and queue is full message will be dropped
			if err := m.consensus.Receive(nil, []*types.Message{msg}); err != nil {
				m.logger.Warn("consensus doesn't accept messages. error=", err)
			}
			return nil
		})
	})
	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case vals := <-m.consensus.Learned():
				m.bus.send(ctx, vals)
			}
		}
	})
	return group.Wait()
}

func (m *Manager) Propose(ctx context.Context, value *types.Value) error {
	return m.consensus.Propose(ctx, value)
}

// TODO subsriber should be able to specify first non-applied value, and manager should fetch
// all available values from persistent store and send to subscriber.
func (m *Manager) Subscribe(ctx context.Context, values chan<- []*types.LearnedValue) {
	m.bus.subscribe(ctx, values)
}

type sub struct {
	ctx    context.Context
	values chan<- []*types.LearnedValue
}

func newBus() *valuesBus {
	return &valuesBus{
		subs: map[int]*sub{},
	}
}

type valuesBus struct {
	mu   sync.RWMutex
	subs map[int]*sub
	last int
}

func (bus *valuesBus) subscribe(ctx context.Context, values chan<- []*types.LearnedValue) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	bus.subs[bus.last] = &sub{
		ctx:    ctx,
		values: values,
	}
	bus.last++
}

func (bus *valuesBus) send(ctx context.Context, vals []*types.LearnedValue) {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	for i, sub := range bus.subs {
		select {
		case <-ctx.Done():
		case <-sub.ctx.Done():
			delete(bus.subs, i)
		case sub.values <- vals:
		}
	}
}
