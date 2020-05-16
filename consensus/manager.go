package consensus

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dshulyak/rapid/consensus/types"
	atypes "github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrNetwork raised for any issue related to network IO while sending/receiving messages.
	ErrNetwork = errors.New("network is unreliable")
)

type ConsumeFn func(context.Context, *types.Message) error

type NetworkService interface {
	Send(context.Context, *types.Message) error
	Register(ConsumeFn)
	Update(*atypes.Changes) error
}

func NewManager(logger *zap.SugaredLogger, swarm NetworkService, conf Config, tick time.Duration) *Manager {
	cons := NewConsensus(logger, NewPaxos(logger, conf), tick)
	swarm.Register(func(ctx context.Context, msg *types.Message) error {
		if err := cons.Receive(ctx, []*types.Message{msg}); err != nil {
			cons.logger.With("error", err).Warn("consensus failed to accept messages.")
		}
		return nil
	})
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
	swarm     NetworkService

	bus *valuesBus
}

func (m *Manager) Run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer m.logger.Info("exit consensus reactor")
		return m.consensus.Run(ctx)
	})
	swarmvals := make(chan []*types.LearnedValue, 1)
	m.bus.subscribe(ctx, swarmvals)
	group.Go(func() error {
		defer m.logger.Info("exit network updater")
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case vals := <-swarmvals:
				for _, v := range vals {
					m.swarm.Update(v.Value.Changes)
				}
			}
		}
	})
	group.Go(func() error {
		defer m.logger.Info("exit messages sender")
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case msgs := <-m.consensus.Messages():
				// TODO each message should be sent to separate channel to prevent blocking here.
				for i := range msgs {
					if err := m.swarm.Send(ctx, msgs[i]); err != nil && !errors.Is(err, context.Canceled) {
						m.logger.With("error", err).Debug("failed to send")
					}
				}
			}
		}
	})
	group.Go(func() error {
		defer m.logger.Info("exit values observer")
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
