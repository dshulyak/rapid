package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/swarms/grpc/service"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	ctypes "github.com/dshulyak/rapid/consensus/types"
)

func New(logger *zap.SugaredLogger, node *types.Node, config *types.Configuration) *Swarm {
	nodes := map[uint64]*types.Node{}
	for _, n := range config.Nodes {
		nodes[n.ID] = n
	}
	return &Swarm{
		logger: logger.Named("grpc"),
		node:   node,
		config: nodes,
		pool:   map[uint64]service.ConsensusClient{},
	}
}

// TODO big issue. connections are not closed.
// define contracts for closing unused connections and general cleanup.
type Swarm struct {
	logger *zap.SugaredLogger

	node   *types.Node
	config map[uint64]*types.Node

	// consuming is set to 1 if consumer was started.
	consuming int32

	mu   sync.RWMutex
	pool map[uint64]service.ConsensusClient
}

func (s *Swarm) Update(changes *types.Changes) error {
	if changes == nil {
		return nil
	}
	if len(changes.List) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, change := range changes.List {
		switch change.Type {
		case types.Change_JOIN:
			s.config[change.Node.ID] = change.Node
		case types.Change_REMOVE:
			delete(s.config, change.Node.ID)
			delete(s.pool, change.Node.ID)
		}
	}
	return nil
}

func (s *Swarm) Send(ctx context.Context, msg *ctypes.Message) error {
	s.mu.RLock()
	if client, exist := s.pool[msg.To]; exist {
		defer s.mu.RUnlock()
		// TODO send options?
		_, err := client.Send(ctx, msg)
		return err
	}
	s.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	// concurrent goroutines can both wait on s.mu.Lock
	if client, exist := s.pool[msg.To]; exist {
		_, err := client.Send(ctx, msg)
		return err
	}
	conf, exist := s.config[msg.To]
	if !exist {
		return fmt.Errorf("no address for peer %d", msg.To)
	}
	addr := fmt.Sprintf("%s:%d", conf.IP, conf.Port)
	conn, err := grpc.DialContext(ctx, addr)
	if err != nil {
		return fmt.Errorf("failed to dial peer %d: %w", msg.To, err)
	}
	client := service.NewConsensusClient(conn)
	s.pool[msg.To] = client
	_, err = client.Send(ctx, msg)
	return err
}

func (s *Swarm) Consume(ctx context.Context, fn consensus.ConsumeFn) error {
	if !atomic.CompareAndSwapInt32(&s.consuming, 0, 1) {
		return errors.New("already consuming")
	}
	defer atomic.StoreInt32(&s.consuming, 0)

	sock, err := net.Listen("tcp", fmt.Sprintf("%s:%d", s.node.IP, s.node.Port))
	if err != nil {
		return err
	}

	// TODO pass server options during initialization
	srv := grpc.NewServer()
	service.RegisterConsensusServer(srv, consumer(fn))

	errchan := make(chan error, 1)
	go func() {
		errchan <- srv.Serve(sock)
	}()
	select {
	case <-ctx.Done():
		srv.Stop()
		return nil
	case err := <-errchan:
		return err
	}
}

var _ service.ConsensusServer = (consumer)(nil)

type consumer consensus.ConsumeFn

func (cn consumer) Send(ctx context.Context, message *ctypes.Message) (*service.Empty, error) {
	if err := cn(ctx, message); err != nil {
		return nil, err
	}
	return nil, nil
}
