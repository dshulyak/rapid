package monitor

import (
	"github.com/dshulyak/rapid/graph"
	"github.com/dshulyak/rapid/types"
)

type LastKG struct {
	kparam int
	conf   *types.LastConfiguration
}

func (kg LastKG) Last() (*graph.KGraph, *types.Configuration, <-chan struct{}) {
	conf, update := kg.conf.Last()
	return graph.New(kg.kparam, conf.Nodes), conf, update
}
