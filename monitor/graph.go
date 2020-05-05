package monitor

import (
	"encoding/binary"
	"hash"
	"hash/fnv"
	"sort"

	"github.com/dshulyak/rapid/types"
)

func NewKGraph(k int, nodes []*types.Node) *KGraph {
	lth := len(nodes)
	graphs := make([]*graph, k)
	hasher := fnv.New64()
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
			prefix:    byte(i),
			observers: map[uint64]uint64{},
			subjects:  map[uint64]uint64{},
		}
		sort.Slice(tmp, func(i, j int) bool {
			ibuf := make([]byte, 9)
			jbuf := make([]byte, 9)
			ibuf[0] = g.prefix
			jbuf[0] = g.prefix
			binary.BigEndian.PutUint64(ibuf[1:], tmp[i])
			binary.BigEndian.PutUint64(jbuf[1:], tmp[j])

			_, _ = hasher.Write(ibuf)
			isum := hasher.Sum64()
			hasher.Reset()

			_, _ = hasher.Write(jbuf)
			jsum := hasher.Sum64()
			return isum < jsum
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
	prefix              byte
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
