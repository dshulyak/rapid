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

	seen map[uint64]uint64

	lastSeqNum uint64

	wg    sync.WaitGroup
	peers map[uint64]peer

	kg     *graph.KGraph
	update <-chan struct{}

	watching chan []*types.BroadcastMessage
	received []*types.BroadcastMessage
}

func (s *broadcastState) isNew(msg *types.BroadcastMessage) bool {
	if msg.SeqNum <= s.seen[msg.From] {
		return false
	}
	s.seen[msg.From] = msg.SeqNum
	return true
}

func (s *broadcastState) nextSeqNum() uint64 {
	next := s.lastSeqNum
	s.lastSeqNum++
	s.seen[s.conf.NodeID] = next
	return next
}

func (s *broadcastState) reorg() {
	for id := range s.peers {
		old := true
		s.kg.IterateObservers(s.conf.NodeID, func(n *types.Node) bool {
			if n.ID == id {
				old = false
				return false
			}
			return true
		})
		if old {
			s.peers[id].Stop()
			delete(s.peers, id)
		}
	}
	s.kg.IterateObservers(s.conf.NodeID, func(n *types.Node) bool {
		n = n
		if _, exist := s.peers[n.ID]; exist {
			return true
		}
		s.logger.With(
			"node", n,
		).Debug("created broadcaster")

		ctx, cancel := context.WithCancel(s.ctx)
		p := peer{
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
		s.peers[n.ID] = p

		s.wg.Add(1)
		go func() {
			err := p.Run(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				s.logger.With(
					"node", n,
				).Debug("exited broadcaster")
			} else {
				s.logger.With(
					"node", n,
					"error", err,
				).Error("broadcaster failed")
			}
			s.wg.Done()
		}()
		return true
	})
}
