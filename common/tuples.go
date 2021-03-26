package common

import (
	"bytes"
	"errors"
)

// Tuples, eg [(index, bucket, key, value)]
type Tuples struct {
	Values [][]byte
	SortBy int
	Length int
	arity  int
}

func NewTuples(size, arity, sortBy int) *Tuples {
	return &Tuples{
		Values: make([][]byte, 0, size*arity),
		Length: 0,
		SortBy: sortBy,
		arity:  arity,
	}
}

func (t *Tuples) Append(values ...[]byte) error {
	if len(values) != t.arity {
		return errors.New("got an incorrect number of values")
	}

	t.Length++

	t.Values = append(t.Values, values...)

	return nil
}

func (t Tuples) Len() int {
	return t.Length
}

func (t Tuples) Less(i, j int) bool {
	return bytes.Compare(t.Values[i*t.arity+t.SortBy], t.Values[j*t.arity+t.SortBy]) == -1
}

func (t Tuples) Swap(i, j int) {
	for index := 0; index < t.arity; index++ {
		t.Values[i*t.arity+index], t.Values[j*t.arity+index] = t.Values[j*t.arity+index], t.Values[i*t.arity+index]
	}
}
