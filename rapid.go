package rapid

import (
	"github.com/dshulyak/rapid/bootstrap"
	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/monitor"
	"go.uber.org/zap"
)

type Rapid struct {
	logger *zap.SugaredLogger

	seed bootstrap.Client
	mon  *monitor.Manager
	cons *consensus.Manager
}
