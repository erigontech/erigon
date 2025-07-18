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

package aura

import (
	"container/list"

	"github.com/erigontech/erigon-lib/common"
)

// nolint
type unAssembledHeader struct {
	hash    common.Hash
	number  uint64
	signers []common.Address
}
type unAssembledHeaders struct {
	l *list.List
}

func (u unAssembledHeaders) PushBack(header *unAssembledHeader)  { u.l.PushBack(header) }
func (u unAssembledHeaders) PushFront(header *unAssembledHeader) { u.l.PushFront(header) }
func (u unAssembledHeaders) Pop() *unAssembledHeader {
	e := u.l.Front()
	if e == nil {
		return nil
	}
	u.l.Remove(e)
	return e.Value.(*unAssembledHeader)
}
func (u unAssembledHeaders) Front() *unAssembledHeader {
	e := u.l.Front()
	if e == nil {
		return nil
	}
	return e.Value.(*unAssembledHeader)
}
