package consensus

import (
	"github.com/dshulyak/rapid/consensus/types"
	atypes "github.com/dshulyak/rapid/types"
)

// Application doesn't need to store log of values, it is enough to ensure that last agreed value is replicated.
func newValues() *values {
	return &values{values: map[uint64]*types.LearnedValue{}}
}

type values struct {
	// sequence is the last commited sequence number.
	sequence uint64
	values   map[uint64]*types.LearnedValue
}

// Add expects values to be provided in order.
func (vs *values) add(values ...*types.LearnedValue) {
	for _, v := range values {
		vs.values[v.Sequence] = v
	}
}

// Commit expects values to be provided in order.
func (vs *values) commit(value *types.LearnedValue) {
	if value.Sequence <= vs.sequence {
		return
	}
	for i := range vs.values {
		delete(vs.values, i)
	}
	vs.add(value)
	vs.sequence = value.Sequence
}

func (vs *values) get(seq uint64) *types.LearnedValue {
	return vs.values[seq]
}

func (vs *values) commited() uint64 {
	return vs.sequence
}

type replicaState struct {
	id uint64
	// sequence is the last known commited sequence.
	// used to determine if we need to send newer record.
	sequence uint64
	// ticks are used for heartbeat timeout, it ticks are equal to heartbeat timeout
	// coordinator will send out heartbeat to every replica.
	ticks int
}

func newReplicasInfo(nodes []*atypes.Node) replicasInfo {
	info := replicasInfo{
		replicas: map[uint64]*replicaState{},
	}
	for _, n := range nodes {
		info.replicas[n.ID] = &replicaState{id: n.ID}
	}
	return info
}

type replicasInfo struct {
	replicas map[uint64]*replicaState
}

func (info replicasInfo) update(changes *atypes.Changes) {
	if changes == nil {
		return
	}
	for _, change := range changes.List {
		switch change.Type {
		case atypes.Change_JOIN:
			info.replicas[change.Node.ID] = &replicaState{id: change.Node.ID}
		case atypes.Change_REMOVE:
			delete(info.replicas, change.Node.ID)
		}
	}
}

func (info replicasInfo) commited(id uint64) uint64 {
	return info.replicas[id].sequence
}

func (info replicasInfo) commit(id, seq uint64) {
	info.replicas[id].sequence = seq
}

func (info replicasInfo) ticks(id uint64) int {
	return info.replicas[id].ticks
}

func (info replicasInfo) tick(id uint64) {
	info.replicas[id].ticks++
}

func (info replicasInfo) resetTicks(id uint64) {
	info.replicas[id].ticks = 0
}

func (info replicasInfo) iterate(fn func(*replicaState) bool) {
	for _, state := range info.replicas {
		if !fn(state) {
			return
		}
	}
}

func (info replicasInfo) exist(id uint64) bool {
	_, exist := info.replicas[id]
	return exist
}
