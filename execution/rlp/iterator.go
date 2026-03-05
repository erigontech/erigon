// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package rlp

type listIterator struct {
	data []byte
	next []byte
	err  error
}

// NewListIterator creates an iterator for the (list) represented by data
func NewListIterator(data RawValue) (*listIterator, error) {
	k, t, c, err := readKind(data)
	if err != nil {
		return nil, err
	}
	if k != List {
		return nil, ErrExpectedList
	}
	it := &listIterator{
		data: data[t : t+c],
	}
	return it, nil

}

// Next moves the iterator forward by one element.
// It returns true if another item is available, or if this step hit a parse error (check Err()).
// If a parse error happens, the iterator is closed and all future calls will return false.
func (it *listIterator) Next() bool {
	if len(it.data) == 0 {
		return false
	}
	_, t, c, err := readKind(it.data)
	if err != nil {
		it.next = nil
		it.err = err
		// Mark the iterator as done so future Next calls won't loop endlessly.
		it.data = nil
		return true
	}
	it.next = it.data[:t+c]
	it.data = it.data[t+c:]
	it.err = nil
	return true
}

// Value returns the current value
func (it *listIterator) Value() []byte {
	return it.next
}

func (it *listIterator) Err() error {
	return it.err
}
