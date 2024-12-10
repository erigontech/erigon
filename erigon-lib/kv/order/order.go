// Copyright 2021 The Erigon Authors
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

package order

import (
	"bytes"
	"fmt"

	"github.com/erigontech/erigon-lib/common/dbg"
)

type By bool

const (
	Asc  By = true
	Desc By = false
)

func FromBool(v bool) By {
	if v {
		return Asc
	}
	return Desc
}

func (asc By) Assert(k1, k2 []byte) {
	if !dbg.AssertEnabled {
		return
	}
	if k1 == nil || k2 == nil {
		return
	}
	if asc {
		if bytes.Compare(k1, k2) > 0 {
			panic(fmt.Sprintf("epect: %x <= %x", k1, k2))
		}
		return
	}
	if bytes.Compare(k1, k2) < 0 {
		panic(fmt.Sprintf("epect: %x >= %x", k1, k2))
	}
}

func (asc By) AssertList(keys [][]byte) {
	if !dbg.AssertEnabled {
		return
	}
	if len(keys) < 2 {
		return
	}
	for i := 0; i < len(keys)-2; i++ {
		asc.Assert(keys[i], keys[i+1])
	}
}
