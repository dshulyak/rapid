package consensus

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

func (sr *SelfBroadcast) Messages() (rst []MessageTo) {
	msgs := sr.Backend.Messages()
	rst = append(rst, msgs...)
	for msgs != nil {
		for _, msg := range msgs {
			if len(msg.To) == 0 {
				sr.Backend.Step(MessageFrom{
					From:    sr.conf.ReplicaID,
					Message: msg.Message,
				})
			}
		}
		msgs = sr.Backend.Messages()
		rst = append(rst, msgs...)
	}
	return rst
}
