package inproc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/types"

	atypes "github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
)

func NewSwarm(logger *zap.SugaredLogger, network *Network, id uint64) *Swarm {
	return &Swarm{
		logger:  logger.Named("swarm").With("node", id),
		id:      id,
		network: network,
	}
}

var _ consensus.Swarm = (*Swarm)(nil)

type Swarm struct {
	logger  *zap.SugaredLogger
	id      uint64
	network *Network
}

func (s *Swarm) Update(changes *atypes.Changes) error {
	s.logger.Debug("applying changes=", changes)
	return nil
}

func (s *Swarm) Send(ctx context.Context, msg *types.Message) error {
	if msg.From == msg.To {
		return fmt.Errorf("sending message to the same node not supported %d", msg.From)
	}
	p, err := s.network.connect(s.id, msg.To)
	if err != nil {
		return err
	}
	//s.logger.Debug("send=", msg)
	if err := p.send(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (s *Swarm) Register(fn consensus.ConsumeFn) {
	s.logger.Debug("register messages consumer")
	s.network.register(s.id, func(ctx context.Context, msg *types.Message) error {
		//s.logger.Debug("received=", msg)
		return fn(ctx, msg)
	})
}

func newPipe(ctx context.Context, from, to uint64) *pipe {
	return &pipe{
		ctx:  ctx,
		from: from,
		to:   to,
		// TODO reconsider using two buffers when framework will be extended with error conditions
		messages: make(chan *types.Message, 10),
	}
}

type pipe struct {
	ctx      context.Context
	from, to uint64
	messages chan *types.Message
}

func (p *pipe) send(ctx context.Context, msg *types.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.messages <- msg:
		return nil
	}

}

func (p *pipe) run(fn consensus.ConsumeFn) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case msg := <-p.messages:
			_ = fn(context.Background(), msg)
		}
	}
}

func NewNetwork() *Network {
	ctx, cancel := context.WithCancel(context.Background())
	return &Network{
		ctx:       ctx,
		cancel:    cancel,
		pipes:     map[uint64]map[uint64]*pipe{},
		consumers: map[uint64]consensus.ConsumeFn{},
	}
}

// Network must be shared between all participants in the swarm.
type Network struct {
	ctx    context.Context
	cancel func()

	mu        sync.Mutex
	wg        sync.WaitGroup // wg.Add is not concurrency safe
	pipes     map[uint64]map[uint64]*pipe
	consumers map[uint64]consensus.ConsumeFn
}

func (n *Network) connect(from, to uint64) (*pipe, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	local, exist := n.consumers[from]
	if !exist {
		return nil, errors.New("local consumer is not ready")
	}

	remote, exist := n.consumers[to]
	if !exist {
		return nil, errors.New("remote consumer is not ready")
	}

	if n.enabled(from, to) {
		return n.pipes[from][to], nil
	}
	_ = n.enabled(to, from)

	topipe := n.pipes[from][to]
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		topipe.run(remote)
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.pipes[from], to)
	}()

	frompipe := n.pipes[to][from]
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		frompipe.run(local)
		n.mu.Lock()
		defer n.mu.Unlock()
		delete(n.pipes[to], from)
	}()
	return n.pipes[from][to], nil
}

func (n *Network) enabled(from, to uint64) bool {
	_, exist := n.pipes[from]
	if !exist {
		n.pipes[from] = map[uint64]*pipe{}
	}
	_, exist = n.pipes[from][to]
	if !exist {
		n.pipes[from][to] = newPipe(n.ctx, from, to)
	}
	return exist
}

func (n *Network) register(id uint64, consumer consensus.ConsumeFn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.consumers[id] = consumer
}

func (n *Network) Stop() {
	n.cancel()
	n.wg.Wait()
}
