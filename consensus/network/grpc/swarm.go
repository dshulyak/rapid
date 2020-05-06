package grpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/network/grpc/service"
	ctypes "github.com/dshulyak/rapid/consensus/types"
	"github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func New(logger *zap.SugaredLogger, srv *grpc.Server, config *types.Configuration) *Swarm {
	nodes := map[uint64]*types.Node{}
	for _, n := range config.Nodes {
		nodes[n.ID] = n
	}
	return &Swarm{
		srv:    srv,
		logger: logger.Named("grpc"),
		config: nodes,
		pool:   map[uint64]service.ConsensusClient{},
	}
}

// TODO big issue. connections are not closed.
// define contracts for closing unused connections and general cleanup.
type Swarm struct {
	srv    *grpc.Server
	logger *zap.SugaredLogger

	config map[uint64]*types.Node

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

func (s *Swarm) Register(fn consensus.ConsumeFn) {
	service.RegisterConsensusServer(s.srv, consumer(fn))
}

var _ service.ConsensusServer = (consumer)(nil)

type consumer consensus.ConsumeFn

func (cn consumer) Send(ctx context.Context, message *ctypes.Message) (*service.Empty, error) {
	if err := cn(ctx, message); err != nil {
		return nil, err
	}
	return nil, nil
}
