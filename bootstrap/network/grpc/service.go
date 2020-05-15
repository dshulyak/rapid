package grpc

import (
	"context"

	"github.com/dshulyak/rapid/bootstrap"
	"github.com/dshulyak/rapid/bootstrap/network/grpc/service"
	"github.com/dshulyak/rapid/types"
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

func (w wrapper) Configuration(ctx context.Context, req *service.Empty) (*types.Configuration, error) {
	return w.Service.Configuration(), nil
}
