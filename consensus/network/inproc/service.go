package inproc

import (
	"context"
	"fmt"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/types"
	"github.com/dshulyak/rapid/network/inproc"

	atypes "github.com/dshulyak/rapid/types"

	"go.uber.org/zap"
)

const (
	sendCode uint64 = 1
)

func New(logger *zap.SugaredLogger, network *inproc.Network, id uint64) *Service {
	return &Service{
		logger:  logger.Named("swarm").With("node", id),
		id:      id,
		network: network,
	}
}

var _ consensus.NetworkService = (*Service)(nil)

type Service struct {
	logger  *zap.SugaredLogger
	id      uint64
	network *inproc.Network
}

func (s *Service) Update(changes *atypes.Changes) error {
	s.logger.With("changes", changes).Debug("applying changes")
	return nil
}

func (s *Service) Send(ctx context.Context, msg *types.Message) error {
	if msg.From == msg.To {
		return fmt.Errorf("sending message to the same node not supported %d", msg.From)
	}
	req := inproc.Request{
		Context: ctx,
		From:    msg.From,
		To:      msg.To,
		Code:    sendCode,
		Object:  msg,
	}
	return s.network.Send(req)
}

func (s *Service) Register(fn consensus.ConsumeFn) {
	s.logger.Debug("registered messages consumer")
	s.network.Register(s.id, sendCode, func(ctx context.Context, msg interface{}) *inproc.Response {
		fn(ctx, msg.(*types.Message))
		return nil
	})
}