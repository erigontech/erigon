package common

import (
	"bytes"
	"errors"
)

// Tuple, eg [(index, bucket, key, value)]
type Tuple struct {
	Values [][]byte
	SortBy int
	Length int
	N      int
}

func NewTuples(size, n, sortBy int) *Tuple {
	return &Tuple{
		Values: make([][]byte, 0, size*n),
		Length: 0,
		SortBy: sortBy,
		N:      n,
	}
}

func (t *Tuple) Append(values ...[]byte) error {
	if len(values) != t.N {
		return errors.New("got an incorrect number of values")
	}

	t.Length++

	for _, value := range values {
		t.Values = append(t.Values, value)
	}
	return nil
}

func (t Tuple) Len() int {
	return t.Length
}

func (t Tuple) Less(i, j int) bool {
	return bytes.Compare(t.Values[i*t.N+t.SortBy], t.Values[j*t.N+t.SortBy]) == -1
}

func (t Tuple) Swap(i, j int) {
	for index := 0; index < t.N; index++ {
		t.Values[i*t.N+index] = t.Values[j*t.N+index]
	}
}
