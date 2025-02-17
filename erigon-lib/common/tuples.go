// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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
