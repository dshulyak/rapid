package inproc

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Handler func(context.Context, interface{}) *Response

type Request struct {
	context.Context
	From, To uint64
	Code     uint64
	Object   interface{}
	Response chan<- *Response
}

type Response struct {
	Object interface{}
	Err    error
}

func newPipe(ctx context.Context, from, to uint64, applied []Policy) *pipe {
	return &pipe{
		ctx:  ctx,
		from: from,
		to:   to,
		// TODO reconsider using two buffers when framework will be extended with error conditions
		messages: make(chan Request, 10),
		// intentionally not buffered, to guarantee that next message will always will be received after policy was applied
		policies: make(chan Policy),
		applied:  applied,
	}
}

type pipe struct {
	ctx      context.Context
	from, to uint64
	policies chan Policy
	applied  []Policy
	messages chan Request
}

func (p *pipe) send(msg Request) error {
	select {
	case <-msg.Done():
		return msg.Err()
	case p.messages <- msg:
		return nil
	}

}

func (p *pipe) run(handlers map[uint64]Handler) {
	for {
		select {
		case <-p.ctx.Done():
			return
		case policy := <-p.policies:
			switch policy.(type) {
			case Cancel:
				p.applied = nil
			default:
				p.applied = append(p.applied, policy)
			}
		case msg := <-p.messages:
			drop := false
			for _, policy := range p.applied {
				switch typed := policy.(type) {
				case DropPolicy:
					if typed.Drop(p.from, p.to, msg.Object) {
						drop = true
						break
					}
				}
			}
			if drop {
				continue
			}
			handler, exist := handlers[msg.Code]
			if exist {
				resp := handler(msg, msg.Object)
				if msg.Response != nil {
					msg.Response <- resp
				}
			} else {
				panic(fmt.Sprintf("handler for code %d not registered", msg.Code))
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
	applied   []Policy
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

func (n *Network) Send(req Request) error {
	p, err := n.connect(req.From, req.To)
	if err != nil {
		return err
	}

	if err := p.send(req); err != nil {
		return err
	}
	return nil
}

func (n *Network) Apply(policy Policy) {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch policy.(type) {
	case Cancel:
		n.applied = nil
	default:
		n.applied = append(n.applied, policy)
	}
	var wg sync.WaitGroup
	for _, to := range n.pipes {
		for _, pipe := range to {
			pipe := pipe
			wg.Add(1)
			go func() {
				pipe.policies <- policy
				wg.Done()
			}()
		}
	}
	wg.Wait()
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
		n.pipes[from][to] = newPipe(n.ctx, from, to, n.applied)
	}
	return exist
}
