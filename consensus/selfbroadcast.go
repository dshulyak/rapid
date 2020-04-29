package consensus

import "github.com/dshulyak/rapid/consensus/types"

var _ Backend = (*SelfBroadcast)(nil)

func WithSelfBroadcast(conf Config, backend Backend) *SelfBroadcast {
	return &SelfBroadcast{
		Backend: backend,
		conf:    conf,
	}
}

// SelfBroadcast is a decorator to avoid adding inprocess network abstraction to deliver
// broadcast messages to the replica.
type SelfBroadcast struct {
	Backend
	conf Config
}

func (sr *SelfBroadcast) Messages() (rst []*types.Message) {
	msgs := sr.Backend.Messages()
	rst = append(rst, msgs...)

	for _, msg := range msgs {
		if msg.To == sr.conf.ReplicaID {
			sr.Backend.Step(msg)
		}
	}
	msgs = sr.Backend.Messages()
	rst = append(rst, msgs...)

	return rst
}
