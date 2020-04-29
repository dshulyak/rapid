package consensus

import (
	"context"
	"errors"
	"net"

	"github.com/dshulyak/rapid/consensus/types"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrNetwork raised for any issue related to network IO while sending/receiving messages.
	ErrNetwork = errors.New("network is unreliable")
)

type Replica struct {
	IP   net.IP
	Port uint16
	ID   uint64
}

type ConsumeFn func(context.Context, *types.Message) error

type Swarm interface {
	Send(context.Context, *types.Message) error
	Consume(context.Context, ConsumeFn) error
}

func NewManager(cons *Consensus, swarm Swarm) *Manager {
	return &Manager{
		consensus: cons,
		swarm:     swarm,
	}
}

// Manager stitches all subcomponents together (swarm, consensus) and provides simplified API
// to consume consensus subsystem.
type Manager struct {
	consensus *Consensus
	swarm     Swarm
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
				for i := range msgs {
					m.swarm.Send(ctx, msgs[i])
				}
			}
		}
	})
	group.Go(func() error {
		return m.swarm.Consume(ctx, func(_ context.Context, msg *types.Message) error {
			// if context is nil and queue is full message will be dropped
			m.consensus.Receive(nil, []*types.Message{msg})
			return nil
		})
	})
	group.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-m.consensus.Learned():
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
func (m *Manager) Subscribe(ctx context.Context, values chan<- []*types.LearnedValue) {}
