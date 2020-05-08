package inproc

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Handler func(context.Context, interface{})

type request struct {
	ctx    context.Context
	code   uint64
	object interface{}
}

func newPipe(ctx context.Context, from, to uint64) *pipe {
	return &pipe{
		ctx:  ctx,
		from: from,
		to:   to,
		// TODO reconsider using two buffers when framework will be extended with error conditions
		messages: make(chan request, 10),
	}
}

type pipe struct {
	ctx      context.Context
	from, to uint64
	messages chan request
}

func (p *pipe) send(ctx context.Context, msg request) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.messages <- msg:
		return nil
	}

}

func (p *pipe) run(handlers map[uint64]Handler) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case msg := <-p.messages:
			handler, exist := handlers[msg.code]
			if exist {
				handler(msg.ctx, msg.object)
			} else {
				panic(fmt.Sprintf("handler for code %d not registered", msg.code))
			}
		}
	}
}

func NewNetwork() *Network {
	ctx, cancel := context.WithCancel(context.Background())
	return &Network{
		ctx:       ctx,
		cancel:    cancel,
		pipes:     map[uint64]map[uint64]*pipe{},
		consumers: map[uint64]map[uint64]Handler{},
	}
}

// Network must be shared between all participants in the swarm.
type Network struct {
	ctx    context.Context
	cancel func()

	mu        sync.Mutex
	wg        sync.WaitGroup // wg.Add is not concurrency safe
	pipes     map[uint64]map[uint64]*pipe
	consumers map[uint64]map[uint64]Handler
}

func (n *Network) Register(id uint64, code uint64, f Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, exist := n.consumers[id]
	if !exist {
		n.consumers[id] = map[uint64]Handler{}
	}
	if _, exist := n.consumers[id][code]; exist {
		panic(fmt.Sprintf("code %d already registered", code))
	}
	n.consumers[id][code] = f
}

func (n *Network) Stop() {
	n.cancel()
	n.wg.Wait()
}

func (n *Network) Send(ctx context.Context, from, to, code uint64, msg interface{}) error {
	p, err := n.connect(from, to)
	if err != nil {
		return err
	}

	if err := p.send(ctx, request{
		code:   code,
		object: msg,
	}); err != nil {
		return err
	}
	return nil
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
