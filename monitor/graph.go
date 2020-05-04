package monitor

import (
	"math/rand"

	"github.com/dshulyak/rapid/types"
)

func NewKGraph(seed int64, k int, nodes []*types.Node) *KGraph {
	rng := rand.New(rand.NewSource(seed))
	lth := len(nodes)
	graphs := make([]*graph, k)
	for i := range graphs {
		tmp := make([]*types.Node, lth)
		copy(tmp, nodes)
		rng.Shuffle(lth, func(i, j int) {
			tmp[i], tmp[j] = tmp[j], tmp[i]
		})
		g := &graph{
			observers: map[uint64]uint64{},
			subjects:  map[uint64]uint64{},
		}
		for i := 0; i < lth-1; i++ {
			g.add(tmp[i].ID, tmp[i+1].ID)
		}
		g.add(tmp[lth-1].ID, tmp[0].ID)
		graphs[i] = g
	}
	nodemap := map[uint64]*types.Node{}
	for _, n := range nodes {
		nodemap[n.ID] = n
	}
	// TODO there should be a test that good expander test was generated.
	return &KGraph{
		K:      k,
		rng:    rng,
		graphs: graphs,
		nodes:  nodemap,
	}
}

type KGraph struct {
	K      int
	rng    *rand.Rand
	graphs []*graph
	nodes  map[uint64]*types.Node
}

func (k *KGraph) IterateSubjects(u uint64, fn func(*types.Node) bool) {
	for _, g := range k.graphs {
		if !fn(k.nodes[g.subject(u)]) {
			return
		}
	}
}

func (k *KGraph) IterateObservers(v uint64, fn func(*types.Node) bool) {
	for _, g := range k.graphs {
		if !fn(k.nodes[g.observer(v)]) {
			return
		}
	}
}

type graph struct {
	observers, subjects map[uint64]uint64
}

func (g *graph) add(u, v uint64) {
	g.subjects[u] = v
	g.observers[v] = u
}

func (g *graph) subject(u uint64) uint64 {
	return g.subjects[u]
}

func (g *graph) observer(v uint64) uint64 {
	return g.observers[v]
}
