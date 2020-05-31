package types

func WithInstance(instance uint64, msg *Message) *Message {
	msg.InstanceID = instance
	return msg
}

// WithRouting mutates original msg objects, by adding passed from, to fields to the object.
func WithRouting(from, to uint64, msg *Message) *Message {
	msg.From = from
	msg.To = to
	return msg
}

func NewPrepareMessage(ballot, seq uint64) *Message {
	return &Message{
		Type: &Message_Prepare{
			Prepare: &Prepare{
				Ballot:   ballot,
				Sequence: seq,
			},
		},
	}
}

func NewPromiseMessage(ballot, seq, voteBallot uint64, value *Value) *Message {
	return &Message{
		Type: &Message_Promise{
			Promise: &Promise{
				Ballot:     ballot,
				Sequence:   seq,
				VoteBallot: voteBallot,
				Value:      value,
			},
		},
	}
}

func NewAcceptMessage(ballot, seq uint64, value *Value) *Message {
	return &Message{
		Type: &Message_Accept{
			Accept: &Accept{
				Ballot:   ballot,
				Sequence: seq,
				Value:    value,
			},
		},
	}
}

func NewAcceptedMessage(ballot, seq uint64, value *Value) *Message {
	return &Message{
		Type: &Message_Accepted{
			Accepted: &Accepted{
				Ballot:   ballot,
				Sequence: seq,
				Value:    value,
			},
		},
	}
}

func NewLearnedMessage(values ...*LearnedValue) *Message {
	return &Message{
		Type: &Message_Learned{
			Learned: &Learned{
				Values: values,
			},
		},
	}
}
