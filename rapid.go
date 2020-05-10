package rapid

import (
	"github.com/dshulyak/rapid/bootstrap"
	"github.com/dshulyak/rapid/monitor"
)

type Rapid struct {
	seed bootstrap.Client

	mon monitor.Manager
}
