package consensus

import (
	"context"
	"fmt"
	"time"

	"github.com/dshulyak/rapid/network"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

func NewReactor(logger *zap.SugaredLogger,
	bf network.BroadcastFacade,
	configuration *types.LastConfiguration,
	backend *Paxos,
	tick time.Duration) *Reactor {
	return &Reactor{
		logger:        logger.Named("consensus"),
		backend:       backend,
		bf:            bf,
		configuration: configuration,
		tick:          tick,
		proposals:     make(chan *types.Value, 1),
	}
}

// Reactor is thread-safe implementation of the consensus node.
type Reactor struct {
	logger *zap.SugaredLogger
	tick   time.Duration

	bf            network.BroadcastFacade
	configuration *types.LastConfiguration

	backend *Paxos

	proposals chan *types.Value
}

func (c *Reactor) Propose(ctx context.Context, value *types.Value) error {
	select {
	case c.proposals <- value:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (c *Reactor) Run(ctx context.Context) (err error) {
	defer func() {
		if err := recover(); err != nil {
			err = fmt.Errorf("irrecoverable error: %v", err)
			c.logger.With("error", err).Error("exiting")
		}
	}()
	sub, err := c.bf.Subscribe(ctx)
	if err != nil {
		return err
	}
	var (
		egress chan<- []*types.Message
		ticker = time.NewTicker(c.tick)
	)
	defer ticker.Stop()
	defer sub.Stop()

	c.logger.With("period", c.tick).Info("started consensus reactor")
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-ticker.C:
			c.backend.Tick()
		case value := <-c.proposals:
			c.backend.Propose(value)
		case msgs := <-sub.Messages:
			for _, msg := range msgs {
				c.backend.Step(msg)
			}
		case egress <- c.backend.Messages:
			egress = nil
			c.backend.Messages = nil
			if len(c.backend.Values) > 0 {
				for _, val := range c.backend.Values {
					c.configuration.Update(&types.Configuration{
						ID:    val.Sequence,
						Nodes: val.Value.Nodes,
					})
				}
				c.backend.Values = nil
			}
			if len(c.backend.Messages) > 0 {
				egress = c.bf.Egress()
			}
		}
	}
	return
}
