package graph

import (
	"sort"
	"sync/atomic"

	"github.com/dshulyak/rapid/types"
)

func New(k int, nodes []*types.Node) *KGraph {
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
			ordered:   tmp,
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
	seed                uint8
	ordered             []uint64
	observers, subjects map[uint64]uint64
}

// closestLeft returns left peer on the ring.
func (g *graph) closestLeft(u uint64) uint64 {
	return g.closest(u, func(x, y uint64) bool {
		return x > y
	})
}

// closestRight returns first right peer from the ring.
func (g *graph) closestRight(u uint64) uint64 {
	return g.closest(u, func(x, y uint64) bool {
		return x < y
	})
}

func (g *graph) closest(u uint64, cmp func(x, y uint64) bool) uint64 {
	i := 0
	hu := fnvhash64(g.seed, u)
	for i = range g.ordered {
		if cmp(hu, fnvhash64(g.seed, g.ordered[i])) {
			break
		}
	}
	if i == 0 {
		return g.ordered[len(g.ordered)-1]
	}
	return g.ordered[i]
}

func (g *graph) add(u, v uint64) {
	g.subjects[u] = v
	g.observers[v] = u
}

func (g *graph) subject(u uint64) uint64 {
	if v, exist := g.subjects[u]; exist {
		return v
	}
	return g.closestRight(u)
}

func (g *graph) observer(v uint64) uint64 {
	if u, exist := g.observers[v]; exist {
		return u
	}
	return g.closestLeft(v)
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

func NewLast(kg *KGraph) *LastKG {
	last := &LastKG{}
	last.Update(kg)
	return last
}

type notifiable struct {
	kg    *KGraph
	event chan struct{}
}

// LastKG is a utility to broadcast updates.
// When channel notification is received
type LastKG struct {
	value atomic.Value
}

func (c *LastKG) Update(kg *KGraph) {
	current := c.value.Load()
	c.value.Store(notifiable{kg, make(chan struct{})})
	if current != nil {
		close(current.(notifiable).event)
	}
}

func (c *LastKG) Last() (*KGraph, <-chan struct{}) {
	val := c.value.Load().(notifiable)
	return val.kg, val.event
}

func (c *LastKG) Graph() *KGraph {
	return c.value.Load().(notifiable).kg
}
