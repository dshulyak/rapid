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

	watching chan []*types.Message
	received []*types.Message
}

func (s *broadcastState) isNew(msg *types.Message) bool {
	if msg.SeqNum <= s.seen[msg.From] {
		return false
	}
	s.seen[msg.From] = msg.SeqNum
	return true
}

func (s *broadcastState) nextSeqNum() uint64 {
	s.lastSeqNum++
	s.seen[s.conf.NodeID] = s.lastSeqNum
	return s.lastSeqNum
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
			delete(s.seen, s.conf.NodeID)
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
			egress:   make(chan []*types.Message, 1),
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
