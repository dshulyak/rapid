package inproc

import (
	"context"

	"github.com/dshulyak/rapid/monitor"
	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/network/inproc"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
)

const (
	broadcastCode uint64 = 2
	joinCode      uint64 = 3
)

var _ monitor.NetworkService = (*Service)(nil)

func New(logger *zap.SugaredLogger, id uint64, network *inproc.Network) *Service {
	return &Service{
		id:      id,
		logger:  logger,
		network: network,
		graph:   make(chan *monitor.KGraph, 1),
	}
}

type Service struct {
	id uint64

	logger  *zap.SugaredLogger
	network *inproc.Network

	graph chan *monitor.KGraph
}

func (s *Service) Update(ctx context.Context, kg *monitor.KGraph) {
	s.graph <- kg
}

func (s *Service) Broadcast(ctx context.Context, alertsch <-chan []*mtypes.Alert) error {
	var kg *monitor.KGraph
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case kg = <-s.graph:
		case alerts := <-alertsch:
			if kg == nil {
				continue
			}
			kg.IterateObservers(s.id, func(n *types.Node) bool {
				_ = s.network.Send(inproc.Request{
					Context: ctx,
					From:    s.id,
					To:      n.ID,
					Object:  alerts,
					Code:    broadcastCode,
				})
				return true
			})
		}
	}
}

func (s *Service) Register(handler *monitor.NetworkHandler) {
	s.network.Register(s.id, broadcastCode, func(ctx context.Context, msg interface{}) *inproc.Response {
		alerts := msg.([]*mtypes.Alert)
		_ = handler.Broadcast(ctx, alerts)
		return nil
	})
	s.network.Register(s.id, joinCode, func(ctx context.Context, msg interface{}) *inproc.Response {
		jobj := msg.(join)
		err := handler.Join(ctx, jobj.configID, jobj.node)
		return &inproc.Response{Err: err}
	})
}

func (s *Service) Join(ctx context.Context, configID uint64, observer, subject *types.Node) error {
	resp := make(chan *inproc.Response, 1)
	if err := s.network.Send(inproc.Request{
		Context: ctx,
		From:    subject.ID,
		To:      observer.ID,
		Code:    joinCode,
		Object: join{
			configID: configID,
			node:     subject,
		},
		Response: resp,
	}); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-resp:
		return r.Err
	}
}

type join struct {
	configID uint64
	node     *types.Node
}
