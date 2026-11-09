// Copyright 2026 The Erigon Authors
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

import (
	"fmt"
	"reflect"
	"testing"
)

func BenchmarkStructFields(b *testing.B) {
	fields := make([]reflect.StructField, 48)
	for i := range fields {
		fields[i] = reflect.StructField{Name: fmt.Sprintf("Field%d", i), Type: reflect.TypeFor[uint64]()}
	}
	typ := reflect.StructOf(fields)
	theTC.mu.Lock()
	defer theTC.mu.Unlock()
	theTC.next = make(map[typekey]*typeinfo)
	defer func() { theTC.next = nil }()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := structFields(typ); err != nil {
			b.Fatal(err)
		}
	}
}
