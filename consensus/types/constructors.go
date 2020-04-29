package types

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

func NewFailedPromiseMessage(ballot uint64) *Message {
	return &Message{
		Type: &Message_Promise{
			Promise: &Promise{
				Ballot: ballot,
			},
		},
	}
}

func NewPromiseMessage(ballot, seq, voteBallot, commitSeq uint64, value *Value) *Message {
	return &Message{
		Type: &Message_Promise{
			Promise: &Promise{
				Ballot:           ballot,
				Sequence:         seq,
				VoteBallot:       voteBallot,
				CommitedSequence: commitSeq,
				Value:            value,
			},
		},
	}
}

func NewUpdatePromiseMessage(ballot, commitSeq uint64) *Message {
	return &Message{
		Type: &Message_Promise{
			Promise: &Promise{
				Ballot:           ballot,
				CommitedSequence: commitSeq,
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
