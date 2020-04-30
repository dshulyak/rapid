package memory

import (
	"fmt"

	"github.com/dshulyak/rapid/consensus"
	"github.com/dshulyak/rapid/consensus/types"
)

var (
	_ consensus.TransactionalStore = (*Store)(nil)
)

func New() *Store {
	return &Store{
		commited: map[uint64]uint64{},
		values:   map[uint64]*types.LearnedValue{},
	}
}

type Store struct {
	ballot   uint64
	commited map[uint64]uint64

	last   uint64
	values map[uint64]*types.LearnedValue
}

func (s *Store) StartSession() (consensus.StoreSession, error) {
	return s, nil
}

func (s *Store) End() error {
	return nil
}

func (s *Store) GetBallot() (uint64, error) {
	return s.ballot, nil
}

func (s *Store) SetBallot(value uint64) error {
	s.ballot = value
	return nil
}

func (s *Store) UpdateCommited(replica uint64, seq uint64) error {
	s.commited[replica] = seq
	return nil
}

func (s *Store) GetCommited(replica uint64) (uint64, error) {
	val, exist := s.commited[replica]
	if !exist {
		return 0, fmt.Errorf("%w: commit sequnce for replica %d", consensus.ErrNotFound, replica)
	}
	return val, nil
}

func (s *Store) CommitedSequence() (uint64, error) {
	return s.last, nil
}

func (s *Store) AddValues(update ...*types.LearnedValue) error {
	if len(update) == 0 {
		return nil
	}
	for _, value := range update {
		s.values[value.Sequence] = value
	}
	return nil
}

// TODO need a distiction between pending logs and commited logs
func (s *Store) CommitValue(value *types.LearnedValue) error {
	if value.Sequence <= s.last {
		return nil
	}
	if err := s.AddValues(value); err != nil {
		return err
	}
	s.last = value.Sequence
	return nil
}

func (s *Store) GetValue(seq uint64) (*types.LearnedValue, error) {
	if _, exist := s.values[seq]; !exist {
		return nil, fmt.Errorf("%w: can't find value with seq %d", consensus.ErrNotFound, seq)
	}
	return s.values[seq], nil
}
