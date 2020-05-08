package grpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/monitor/network/grpc/service"
	"github.com/dshulyak/rapid/types"
	"google.golang.org/grpc"
)

type Service struct {
	Broadcaster
	srv *grpc.Server
}

func (s *Service) Register(handler monitor.NetworkHandler) {
	service.RegisterMonitorServer(s.srv, handlerWrapper{handler})
}

func (n Service) Join(ctx context.Context, configID uint64, observer, subject *types.Node) error {
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", observer.IP, observer.Port))
	if err != nil {
		return err
	}
	client := service.NewMonitorClient(conn)
	defer conn.Close()
	rsp, err := client.Join(ctx, &service.JoinRequest{
		Node:            subject,
		ConfigurationID: configID,
	})
	if err != nil {
		return err
	}
	switch rsp.Status {
	case service.JoinResponse_OK:
		return nil
	case service.JoinResponse_OLD_CONFIGURATION_ID:
		return monitor.ErrOutdatedConfigID
	}
	return fmt.Errorf("unknown join status %v", rsp.Status)
}

type handlerWrapper struct {
	handler monitor.NetworkHandler
}

func (w handlerWrapper) Join(ctx context.Context, req *service.JoinRequest) (*service.JoinResponse, error) {
	status := service.JoinResponse_OK
	err := w.handler.Join(ctx, req.ConfigurationID, req.Node)
	if err != nil {
		if errors.Is(err, monitor.ErrOutdatedConfigID) {
			status = service.JoinResponse_OLD_CONFIGURATION_ID
		} else {
			return nil, err
		}
	}
	return &service.JoinResponse{Status: status}, nil
}

func (w handlerWrapper) Broadcast(ctx context.Context, req *service.BroadcastRequest) (*service.Empty, error) {
	return nil, w.handler.Broadcast(ctx, req.Alerts)
}
