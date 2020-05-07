package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/monitor/network/grpc/service"
	mtypes "github.com/dshulyak/rapid/monitor/types"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"

	"google.golang.org/grpc"
)

type Broadcaster struct {
	id uint64

	logger *zap.SugaredLogger
	graph  chan *monitor.KGraph

	dialTimeout, sendTimeout time.Duration
}

func (b Broadcaster) Update(kg *monitor.KGraph) {
	b.graph <- kg
}

func (b Broadcaster) Broadcast(ctx context.Context, source <-chan []*mtypes.Alert) error {
	var (
		wg   sync.WaitGroup
		topo = map[uint64]chan<- []*mtypes.Alert{}
	)
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case alerts := <-source:
			for _, ch := range topo {
				ch <- alerts
			}
		case kg := <-b.graph:
			// close previous connection after an update
			kg.IterateObservers(b.id, func(n *types.Node) bool {
				if _, exist := topo[n.ID]; exist {
					return true
				}
				alertch := make(chan []*mtypes.Alert, 1)

				topo[n.ID] = alertch
				wg.Add(1)

				go func() {
					defer wg.Done()
					b.sendLoop(ctx, n, alertch)
				}()
				return true
			})
		}
	}
}

func (b Broadcaster) sendLoop(ctx context.Context, n *types.Node, source <-chan []*mtypes.Alert) {
	var (
		conn   *grpc.ClientConn
		client service.MonitorClient
		err    error
	)
	for {
		select {
		case <-ctx.Done():
			return
		case alerts := <-source:
			if conn == nil {
				conn, err = b.dial(ctx, n)
				if err != nil {
					continue
				}
				client = service.NewMonitorClient(conn)
			}
			ctx, cancel := context.WithTimeout(ctx, b.sendTimeout)
			_, err = client.Broadcast(ctx, &service.BroadcastRequest{Alerts: alerts})
			if err != nil {
				b.logger.Error("failed to broadcast alerts cnt=", len(alerts), " error=", err)
			}
			cancel()
		}
	}
}

func (b Broadcaster) dial(ctx context.Context, n *types.Node) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, b.dialTimeout)
	defer cancel()
	return grpc.DialContext(ctx, fmt.Sprintf("%s:%d", n.IP, n.Port))
}
