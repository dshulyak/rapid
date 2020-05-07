package grpc

import (
	"context"
	"fmt"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/monitor/network/grpc/service"
	"github.com/dshulyak/rapid/types"
	"google.golang.org/grpc"
)

type Network struct {
	Broadcaster
	srv *grpc.Server
}

func (n Network) Join(ctx context.Context, configID uint64, observer, subject *types.Node) error {
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
