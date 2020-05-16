package inproc

type Policy interface{}

type Cancel struct{}

type DropPolicy interface {
	Policy
	Drop(from, to uint64, msg interface{}) bool
}

func NewPartition(partitions ...[]uint64) Partition {
	allowed := map[uint64][]uint64{}
	for _, part := range partitions {
		for _, id := range part {
			allowed[id] = part
		}
	}
	return Partition{
		allowed: allowed,
	}
}

type Partition struct {
	allowed map[uint64][]uint64
}

func (p Partition) Drop(from, to uint64, msg interface{}) bool {
	for _, id := range p.allowed[from] {
		if id == to {
			return false
		}
	}
	return true
}
