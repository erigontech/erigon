package dbutils

import (
	"bytes"
	"errors"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func DecodeChangeSet(b []byte) (*ChangeSet, error) {
	if len(b) == 0 {
		return NewChangeSet(), nil
	}
	buf := bytes.NewBuffer(b)
	h := new(ChangeSet)
	err := rlp.Decode(buf, h)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func EncodeChangeSet(sh *ChangeSet) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := rlp.Encode(buf, sh)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type Change struct {
	Key   []byte
	Value []byte
}

func NewChangeSet() *ChangeSet  {
	return &ChangeSet{
		Changes:make([]Change,0),
	}
}
type ChangeSet struct {
	Changes []Change
}

func (s *ChangeSet) Add(key []byte, value []byte) *ChangeSet {
	s.Changes = append(s.Changes, Change{
		Key:   key,
		Value: value,
	})
	return s
}
func (s *ChangeSet) MultiAdd(changes []Change) *ChangeSet {
	s.Changes = append(s.Changes, changes...)
	return s
}

func (s *ChangeSet) KeyCount() uint32 {
	return uint32(len(s.Changes))
}

func (s *ChangeSet) Walk(f func(k, v []byte) error) error {
	for i := range s.Changes {
		err := f(s.Changes[i].Key, s.Changes[i].Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ChangeSet) FindFirst(k []byte) ([]byte,error) {
	for i := range s.Changes {
		if bytes.Equal(k, s.Changes[i].Key) {
			return s.Changes[i].Value, nil
		}
	}
	return nil, errors.New("not found")
}

func (s *ChangeSet) FindLast(k []byte) ([]byte,error) {
	for i:= len(s.Changes)-1; i>=0; i--{
		if bytes.Equal(k, s.Changes[i].Key) {
			return s.Changes[i].Value, nil
		}
	}
	return nil, errors.New("not found")
}

func (s *ChangeSet) ChangedKeys() (map[string]struct{}) {
	m:=make(map[string]struct{}, len(s.Changes))
	for i := range s.Changes {
		m[string(s.Changes[i].Key)]= struct{}{}
	}
	return m
}
