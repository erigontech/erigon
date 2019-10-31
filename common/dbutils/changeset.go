package dbutils

import (
	"bytes"

	"github.com/ledgerwatch/turbo-geth/rlp"
)

func Decode(b []byte) (ChangeSet, error) {
	if len(b) == 0 {
		return ChangeSet{
			Changes: make([]Change, 0),
		}, nil
	}
	buf := bytes.NewBuffer(b)
	h := ChangeSet{}
	err := rlp.Decode(buf, &h)
	if err != nil {
		return ChangeSet{}, err
	}
	return h, nil
}

func Encode(sh ChangeSet) ([]byte, error) {
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

type ChangeSet struct {
	Changes []Change
}

func (s ChangeSet) Add(key []byte, value []byte) ChangeSet {
	s.Changes = append(s.Changes, Change{
		Key:   key,
		Value: value,
	})
	return s
}
func (s ChangeSet) MultiAdd(changes []Change) ChangeSet {
	s.Changes = append(s.Changes, changes...)
	return s
}

func (s ChangeSet) KeyCount() uint32 {
	return uint32(len(s.Changes))
}

func (s ChangeSet) Walk(f func(k, v []byte) error) error {
	for i := range s.Changes {
		err := f(s.Changes[i].Key, s.Changes[i].Value)
		if err != nil {
			return err
		}
	}
	return nil
}
