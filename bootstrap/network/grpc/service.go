package grpc

import (
	"context"
	"errors"

	"github.com/dshulyak/rapid/bootstrap"
	"github.com/dshulyak/rapid/bootstrap/network/grpc/service"
	"google.golang.org/grpc"
)

var _ bootstrap.NetworkServer = Service{}

func NewService(srv *grpc.Server) Service {
	return Service{srv: srv}
}

type Service struct {
	srv *grpc.Server
}

func (s Service) Register(bsvc *bootstrap.Service) {
	service.RegisterBootstrapServer(s.srv, wrapper{bsvc})
}

type wrapper struct {
	*bootstrap.Service
}

func (w wrapper) Join(ctx context.Context, req *service.JoinRequest) (*service.JoinResponse, error) {
	conf, err := w.Service.Join(ctx, req.NodeID)
	if err != nil {
		if errors.Is(err, bootstrap.ErrNodeIDConflict) {
			return &service.JoinResponse{Status: service.JoinResponse_NODE_ID_CONFLICT}, nil
		}
	}
	return &service.JoinResponse{
		Configuration: conf,
	}, nil
}