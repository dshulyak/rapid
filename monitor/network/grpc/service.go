package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/monitor/network/grpc/service"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func New(logger *zap.SugaredLogger, id uint64, srv *grpc.Server, dialTimeout, sendTimeout time.Duration) Service {
	return Service{
		logger:      logger,
		id:          id,
		srv:         srv,
		dialTimeout: dialTimeout,
		sendTimeout: sendTimeout,
		graph:       make(chan *monitor.KGraph, 1),
	}
}

type Service struct {
	id uint64

	logger *zap.SugaredLogger
	srv    *grpc.Server

	graph chan *monitor.KGraph

	dialTimeout, sendTimeout time.Duration
}

func (s Service) Register(handler monitor.NetworkHandler) {
	service.RegisterMonitorServer(s.srv, handlerWrapper{handler})
}

func (n Service) Join(ctx context.Context, configID uint64, observer, subject *types.Node) error {
	conn, err := n.dial(ctx, observer)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := service.NewMonitorClient(conn)
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
