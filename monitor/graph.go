package monitor

import (
	"hash"
	"sort"

	"github.com/dshulyak/rapid/types"
)

func NewKGraph(k int, nodes []*types.Node) *KGraph {
	lth := len(nodes)
	graphs := make([]*graph, k)
	nodemap := map[uint64]*types.Node{}
	nodeUIDs := make([]uint64, 0, lth)

	for _, n := range nodes {
		nodemap[n.ID] = n
		nodeUIDs = append(nodeUIDs, n.ID)
	}

	for i := range graphs {
		tmp := make([]uint64, lth)
		copy(tmp, nodeUIDs)

		g := &graph{
			seed:      uint8(i + 1),
			observers: map[uint64]uint64{},
			subjects:  map[uint64]uint64{},
		}
		sort.Slice(tmp, func(i, j int) bool {
			return fnvhash64(g.seed, tmp[i]) < fnvhash64(g.seed, tmp[j])
		})

		for i := 0; i < lth-1; i++ {
			g.add(tmp[i], tmp[i+1])
		}
		g.add(tmp[lth-1], tmp[0])
		graphs[i] = g
	}
	return &KGraph{
		K:      k,
		graphs: graphs,
		nodes:  nodemap,
	}
}

type KGraph struct {
	K      int
	graphs []*graph
	nodes  map[uint64]*types.Node
}

func (k *KGraph) IterateSubjects(u uint64, fn func(*types.Node) bool) {
	_, exist := k.nodes[u]
	if !exist {
	}
	for _, g := range k.graphs {
		if !fn(k.nodes[g.subject(u)]) {
			return
		}
	}
}

func (k *KGraph) IterateObservers(v uint64, fn func(*types.Node) bool) {
	_, exist := k.nodes[v]
	if !exist {
	}
	for _, g := range k.graphs {
		if !fn(k.nodes[g.observer(v)]) {
			return
		}
	}
}

type graph struct {
	seed                uint8
	hasher              hash.Hash64
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

// fnv-1 hash
func fnvhash64(seed uint8, data uint64) uint64 {
	const (
		prime  uint64 = 1099511628211
		offset uint64 = 14695981039346656037
	)
	hash := offset

	hash *= prime
	hash ^= uint64(seed)

	for i := 0; i < 8; i++ {
		hash *= prime
		hash ^= data & 255
		data >>= 8
	}
	return hash
}
