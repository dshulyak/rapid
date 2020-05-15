package grpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/monitor/network/grpc/service"
	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/types"

	"google.golang.org/grpc"
)

func (b Service) Update(kg *monitor.KGraph) {
	b.graph <- kg
}

func (b Service) Broadcast(ctx context.Context, source <-chan []*mtypes.Alert) error {
	var (
		wg   sync.WaitGroup
		topo = map[uint64]chan []*mtypes.Alert{}
	)
	for {
		select {
		case <-ctx.Done():
			for id := range topo {
				close(topo[id])
			}
			wg.Wait()
			return ctx.Err()
		case alerts := <-source:
			b.logger.With("alerts", alerts).Debug("broadcast")
			for _, ch := range topo {
				select {
				case ch <- alerts:
				case <-ctx.Done():
					break
				}
			}
		case kg := <-b.graph:
			for id := range topo {
				old := true
				kg.IterateObservers(b.id, func(n *types.Node) bool {
					if n.ID == id {
						old = false
						return false
					}
					return true
				})
				if old {
					close(topo[id])
					delete(topo, id)
				}
			}
			// close previous connection after an update
			kg.IterateObservers(b.id, func(n *types.Node) bool {
				n = n
				if _, exist := topo[n.ID]; exist {
					return true
				}
				b.logger.With(
					"node", n,
				).Debug("created broadcaster")
				alertch := make(chan []*mtypes.Alert, 1)

				topo[n.ID] = alertch
				wg.Add(1)

				go func() {
					defer wg.Done()
					b.sendLoop(ctx, n, alertch)
					b.logger.With(
						"node", n,
					).Debug("exit broadcaster")
				}()
				return true
			})
		}
	}
}

func (b Service) sendLoop(ctx context.Context, n *types.Node, source <-chan []*mtypes.Alert) {
	var (
		conn   *grpc.ClientConn
		client service.MonitorClient
		err    error
		logger = b.logger.With("node", n)
	)
	for alerts := range source {
		if conn == nil {
			conn, err = b.dial(ctx, n)
			if err != nil {
				logger.With("error", err).Debug("dial failed")
				continue
			}
			client = service.NewMonitorClient(conn)
		}
		ctx, cancel := context.WithTimeout(ctx, b.sendTimeout)
		_, err = client.Broadcast(ctx, &service.BroadcastRequest{Alerts: alerts})
		if err != nil {
			logger.With(
				"alerts", alerts,
				"error", err,
			).Debug("failed to broadcast alerts")
		}
		cancel()
	}
	if conn != nil {
		if err := conn.Close(); err != nil {
			logger.With("error", err).Error("failed to close connection")
		}
	}
	logger.Debug("connection closed")
}

func (b Service) dial(ctx context.Context, n *types.Node) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, b.dialTimeout)
	defer cancel()
	return grpc.DialContext(ctx, fmt.Sprintf("%s:%d", n.IP, n.Port), grpc.WithInsecure(), grpc.WithBlock())
}
