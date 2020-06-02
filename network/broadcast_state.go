package network

import (
	"context"
	"errors"
	"sync"

	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

type broadcastState struct {
	ctx    context.Context
	logger *zap.SugaredLogger
	conf   Config

	network Network

	wg          sync.WaitGroup
	connections map[uint64]connection

	kg     *graph.KGraph
	update <-chan struct{}

	seen map[[32]byte]struct{}

	watching chan []*types.BroadcastMessage
	received []*types.BroadcastMessage
}

func (s *broadcastState) reorg() {
	for id := range s.connections {
		old := true
		s.kg.IterateObservers(s.conf.NodeID, func(n *types.Node) bool {
			if n.ID == id {
				old = false
				return false
			}
			return true
		})
		if old {
			s.connections[id].Stop()
			delete(s.connections, id)
		}
	}
	s.kg.IterateObservers(s.conf.NodeID, func(n *types.Node) bool {
		n = n
		if _, exist := s.connections[n.ID]; exist {
			return true
		}
		s.logger.With(
			"node", n,
		).Debug("created broadcaster")

		ctx, cancel := context.WithCancel(s.ctx)
		conn := connection{
			Stop:     cancel,
			node:     n,
			logger:   s.logger.With("peer", n.ID),
			net:      s.network,
			bufsize:  s.conf.QueueSize,
			dtimeout: s.conf.DialTimeout,
			btimeout: s.conf.SendTimeout,
			retry:    s.conf.RetryPeriod,
			egress:   make(chan []*types.BroadcastMessage, 1),
		}
		s.connections[n.ID] = conn

		s.wg.Add(1)
		go func() {
			err := conn.Run(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				s.logger.With(
					"node", n,
				).Debug("exited broadcaster")
			} else {
				s.logger.With(
					"node", n,
					"error", err,
				).Error("broadcaster crashed")
			}
			s.wg.Done()
		}()
		return true
	})
}
