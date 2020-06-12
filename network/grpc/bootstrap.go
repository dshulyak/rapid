package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/dshulyak/rapid/bootstrap"
	"github.com/dshulyak/rapid/network/grpc/service"
	"github.com/dshulyak/rapid/types"
	"google.golang.org/grpc"
)

func (n GRPCNetwork) RegisterBootstrap(svc bootstrap.Service) {
	service.RegisterBootstrapServer(n.srv, bootstrapWrapper{svc})
}

func (n GRPCNetwork) BootstrapClient(ctx context.Context, node *types.Node) (bootstrap.BootstrapClient, error) {
	addr := fmt.Sprintf("%s:%d", node.IP, node.Port)
	ctx, cancel := context.WithTimeout(ctx, n.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %v: %w", node, err)
	}
	return bootstrapClientWrapper{service.NewBootstrapClient(conn)}, nil
}

type bootstrapWrapper struct {
	bootstrap.Service
}

func (w bootstrapWrapper) Configuration(ctx context.Context, _ *service.ConfigurationReq) (*types.Configuration, error) {
	return w.Service.Configuration(), nil
}

func (w bootstrapWrapper) Join(ctx context.Context, req *service.JoinReq) (*service.JoinResp, error) {
	err := w.Service.Join(ctx, req.InstanceID, req.Node)
	if err == nil {
		return &service.JoinResp{Code: service.JoinResp_OK}, nil
	}
	if errors.Is(err, bootstrap.ErrOldConfiguration) {
		return &service.JoinResp{Code: service.JoinResp_OUTDATED}, nil
	}
	return nil, err
}

type bootstrapClientWrapper struct {
	service.BootstrapClient
}

func (cw bootstrapClientWrapper) Configuration(ctx context.Context) (*types.Configuration, error) {
	return cw.BootstrapClient.Configuration(ctx, &service.ConfigurationReq{})
}

func (cw bootstrapClientWrapper) Join(ctx context.Context, instanceID uint64, node *types.Node) error {
	resp, err := cw.BootstrapClient.Join(ctx, &service.JoinReq{
		InstanceID: instanceID,
		Node:       node,
	})
	if err != nil {
		return err
	}
	if resp.Code == service.JoinResp_OUTDATED {
		return bootstrap.ErrOldConfiguration
	}
	return nil
}
