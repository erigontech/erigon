package dbutils

import (
	"bytes"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func Decode(b []byte) (SuffixHistory, error) {
	if len(b) == 0 {
		return SuffixHistory{
			Changes: make([]Change, 0),
		}, nil
	}
	buf := bytes.NewBuffer(b)
	h := SuffixHistory{}
	err := rlp.Decode(buf, &h)
	if err != nil {
		return SuffixHistory{}, err
	}
	return h, nil
}

func Encode(sh SuffixHistory) ([]byte, error) {
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

type SuffixHistory struct {
	Changes []Change
}

func (s SuffixHistory) Add(key []byte, value []byte) SuffixHistory {
	s.Changes = append(s.Changes, Change{
		Key:   key,
		Value: value,
	})
	return s
}
func (s SuffixHistory) MultiAdd(changes []Change) SuffixHistory {
	s.Changes = append(s.Changes, changes...)
	return s
}

func (s SuffixHistory) KeyCount() uint32 {
	return uint32(len(s.Changes))
}

func (s SuffixHistory) Walk(f func(k, v []byte) error) error {
	for i := range s.Changes {
		err := f(s.Changes[i].Key, s.Changes[i].Value)
		if err != nil {
			return err
		}
	}
	return nil
}
