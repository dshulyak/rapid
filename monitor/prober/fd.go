package prober

import (
	"context"
	"fmt"
	"time"

	"github.com/dshulyak/rapid/monitor"
	"github.com/dshulyak/rapid/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var _ monitor.FailureDetector = FailureDetector{}

func NewFailureDetector(logger *zap.SugaredLogger, period, timeout time.Duration, threshold int) FailureDetector {
	return FailureDetector{
		logger:    logger,
		period:    period,
		timeout:   timeout,
		threshold: threshold,
	}
}

type FailureDetector struct {
	logger    *zap.SugaredLogger
	period    time.Duration
	timeout   time.Duration
	threshold int
}

func (fd FailureDetector) Monitor(ctx context.Context, node *types.Node) error {
	var (
		n      = 0
		ticker = time.NewTicker(fd.period)
	)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err := fd.Probe(ctx, node)
			if err != nil {
				n++
			} else {
				n = 0
			}
			if n == fd.threshold {
				return fmt.Errorf("prober threshold (%d) violated: %w", fd.threshold, err)
			}
		}
	}
}

func (fd FailureDetector) Probe(ctx context.Context, node *types.Node) error {
	ctx, cancel := context.WithTimeout(ctx, fd.timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("%s:%d", node.IP, node.Port))
	if err != nil {
		return err
	}
	return conn.Close()
}
