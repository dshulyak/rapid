package memory

import (
	"context"
	"fmt"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/types"
)

var (
	_ consensus.Persistence = (*Store)(nil)
)

func New() *Store {
	return &Store{
		commited: map[uint64]uint64{},
	}
}

type Store struct {
	ballot   uint64
	commited map[uint64]uint64

	last uint64
	logs []*types.LearnedValue
}

func (s *Store) BeginSession() error {
	return nil
}

func (s *Store) EndSession() error {
	return nil
}

func (s *Store) GetBallot(_ context.Context) (uint64, error) {
	return s.ballot, nil
}

func (s *Store) SetBallot(_ context.Context, value uint64) error {
	s.ballot = value
	return nil
}

func (s *Store) UpdateCommited(_ context.Context, replica uint64, seq uint64) error {
	s.commited[replica] = seq
	return nil
}

func (s *Store) GetCommited(_ context.Context, replica uint64) (uint64, error) {
	val, exist := s.commited[replica]
	if !exist {
		return 0, fmt.Errorf("%w: commit sequnce for replica %d", consensus.ErrNotFound, replica)
	}
	return val, nil
}

func (s *Store) UpdateLastLogCommited(_ context.Context, value uint64) error {
	s.last = value
	return nil
}

func (s *Store) LastLogCommited(context.Context) (uint64, error) {
	return s.last, nil
}

func (s *Store) AddLogs(_ context.Context, update []*types.LearnedValue) error {
	if len(update) == 0 {
		return nil
	}
	last := update[len(update)-1].Sequence
	if last > uint64(len(s.commited)) {
		logs := make([]*types.LearnedValue, last)
		copy(logs, s.logs)
		s.logs = logs
	}
	for _, log := range update {
		s.logs[int(log.Sequence)-1] = log
	}
	return nil
}

// TODO need a distiction between pending logs and commited logs
func (s *Store) CommitLogs(ctx context.Context, update []*types.LearnedValue) error {
	return s.AddLogs(ctx, update)
}

func (s *Store) GetLog(ctx context.Context, seq uint64) (*types.LearnedValue, error) {
	if uint64(len(s.logs)) < seq {
		return nil, fmt.Errorf("%w: can't find value with seq %d", consensus.ErrNotFound, seq)
	}
	return s.logs[int(seq)-1], nil
}

func (s *Store) GetLogs(ctx context.Context, lo, hi uint64) ([]*types.LearnedValue, error) {
	if uint64(len(s.logs)) < hi {
		return nil, fmt.Errorf("%w: can't find value with seq %d", consensus.ErrNotFound, hi)
	}
	logs := make([]*types.LearnedValue, hi-lo)
	copy(logs, s.logs[lo:hi])
	return logs, nil
}
