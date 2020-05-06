package grpc

import (
	"github.com/dshulyak/rapid/monitor"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Server struct {
	logger *zap.SugaredLogger
	src    *grpc.Server

	kg *monitor.KGraph
}
