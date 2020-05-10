package consensus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dshulyak/rapid/consensus/types"
	"go.uber.org/zap"
)

type Backend interface {
	Tick()
	Step(*types.Message)

	Propose(*types.Value)

	Messages() []*types.Message
	Values() []*types.LearnedValue
}

func NewConsensus(logger *zap.SugaredLogger, backend Backend, tick time.Duration) *Consensus {
	return &Consensus{
		logger:    logger.Named("consensus"),
		backend:   backend,
		tick:      tick,
		proposals: make(chan *types.Value, 1),
		ingress:   make(chan []*types.Message, 100), // this is used as a fifo queue, overflow messages will be dropped with error
		egress:    make(chan []*types.Message, 1),
		values:    make(chan []*types.LearnedValue, 1),
	}
}

// Consensus is thread-safe implementation of the consensus node.
type Consensus struct {
	logger *zap.SugaredLogger
	tick   time.Duration

	backend Backend

	proposals chan *types.Value

	ingress chan []*types.Message
	egress  chan []*types.Message

	values chan []*types.LearnedValue
}

func (c *Consensus) Receive(ctx context.Context, msgs []*types.Message) error {
	if ctx != nil {
		select {
		case c.ingress <- msgs:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case c.ingress <- msgs:
	default:
		return errors.New("input queue is full")
	}
	return nil
}

func (c *Consensus) Propose(ctx context.Context, value *types.Value) error {
	select {
	case c.proposals <- value:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (c *Consensus) Learned() <-chan []*types.LearnedValue {
	return c.values
}

func (c *Consensus) Messages() <-chan []*types.Message {
	return c.egress
}

func (c *Consensus) Run(ctx context.Context) (err error) {
	defer func() {
		if err := recover(); err != nil {
			err = fmt.Errorf("irrecoverable error: %v", err)
			c.logger.With("error", err).Error("exiting")
		}
	}()
	var (
		outmsgs []*types.Message
		egress  chan []*types.Message
		ticker  = time.NewTicker(c.tick)
		values  []*types.LearnedValue
		valchan chan []*types.LearnedValue
	)
	defer ticker.Stop()

	c.logger.With("tick", c.tick).Info("starting consensus with ticker period")
	for {
		// TODO if either outmsgs or values grow out of bounds (define in constructor)
		// consensus should fail with irrecoverable error
		if len(outmsgs) > 0 {
			egress = c.egress
		} else {
			egress = nil
		}
		if len(values) > 0 {
			valchan = c.values
		} else {
			valchan = nil
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-ticker.C:
			c.backend.Tick()
			outmsgs = append(outmsgs, c.backend.Messages()...)
		case value := <-c.proposals:
			c.backend.Propose(value)
			outmsgs = append(outmsgs, c.backend.Messages()...)
		case inmsgs := <-c.ingress:
			for i := range inmsgs {
				c.backend.Step(inmsgs[i])
			}
			outmsgs = append(outmsgs, c.backend.Messages()...)
			values = append(values, c.backend.Values()...)
		case egress <- outmsgs:
			outmsgs = nil
		case valchan <- values:
			values = nil
		}
	}
	return
}
