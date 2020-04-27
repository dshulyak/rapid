package consensus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dshulyak/rapid/consensus/types"
)

type Backend interface {
	Tick()
	Step(MessageFrom)

	Propose(*types.Value) error

	Messages() []MessageTo
	Values() []*types.LearnedValue
}

func NewConsensus(backend Backend, tick time.Duration) *Consensus {
	return &Consensus{
		backend:   backend,
		tick:      tick,
		proposals: make(chan *types.Value, 1),
		ingress:   make(chan []MessageFrom, 100), // this is used as a fifo queue, overflow messages will be dropped with error
		egress:    make(chan []MessageTo, 1),
		values:    make(chan []*types.LearnedValue, 1),
	}
}

// Consensus is thread-safe implementation of the consensus node.
type Consensus struct {
	tick time.Duration

	backend Backend

	proposals chan *types.Value

	ingress chan []MessageFrom
	egress  chan []MessageTo

	values chan []*types.LearnedValue
}

func (c *Consensus) Receive(ctx context.Context, msgs []MessageFrom) error {
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

func (c *Consensus) Messages() <-chan []MessageTo {
	return c.egress
}

func (c *Consensus) Run(ctx context.Context) (err error) {
	defer func() {
		if err := recover(); err != nil {
			err = fmt.Errorf("irrecoverable error: %v", err)
		}
	}()
	var (
		outmsgs []MessageTo
		egress  chan []MessageTo
		ticker  = time.NewTicker(c.tick)
		values  []*types.LearnedValue
		valchan chan []*types.LearnedValue
	)
	defer ticker.Stop()

	for {
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
