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

package eliasfano32

import (
	"bytes"
	"testing"
)

// go test -trimpath -v -fuzz=FuzzSingleEliasFano ./recsplit/eliasfano32
// go test -trimpath -v -fuzz=FuzzDoubleEliasFano ./recsplit/eliasfano32

func FuzzSingleEliasFano(f *testing.F) {
	f.Fuzz(func(t *testing.T, in []byte) {
		if len(in)%2 == 1 {
			t.Skip()
		}
		if len(in) == 0 {
			t.Skip()
		}
		for len(in) < int(2*superQ) { // make input large enough to trigger supreQ jump logic
			in = append(in, in...)
		}

		// Treat each byte of the sequence as difference between previous value and the next
		count := len(in)
		keys := make([]uint64, count+1)
		for i, b := range in {
			keys[i+1] = keys[i] + uint64(b)
		}
		ef := NewEliasFano(uint64(count+1), keys[count])
		for _, c := range keys {
			ef.AddOffset(c)
		}
		ef.Build()

		// Try to read from ef
		for i := range keys {
			if ef.Get(uint64(i)) != keys[i] {
				t.Fatalf("i %d: got %d, expected %d", i, ef.Get(uint64(i)), keys[i])
			}
		}

		var i int
		it := ef.Iterator()
		for it.HasNext() {
			v, err := it.Next()
			if err != nil {
				t.Fatalf("it.next: got err: %v", err)
			}
			if v != keys[i] {
				t.Fatalf("it.next: got %d, expected %d", v, keys[i])
			}
			i++
		}
		if i != len(keys) {
			t.Fatalf("it.len: got %d, expected %d", i, len(keys))
		}

		i--
		rit := ef.ReverseIterator()
		for rit.HasNext() {
			v, err := rit.Next()
			if err != nil {
				t.Fatalf("rit.next: got err: %v", err)
			}
			if v != keys[i] {
				t.Fatalf("rit.next: got %d, expected %d, i %d, len, %d", v, keys[i], i, len(keys))
			}
			i--
		}
		if i != -1 {
			t.Fatalf("rit.len: got %d, expected %d", i, -1)
		}

		buf := bytes.NewBuffer(nil)
		err := ef.Write(buf)
		if err != nil {
			t.Fatalf("write: got err: %v", err)
		}
		if ef.Max() != Max(buf.Bytes()) {
			t.Fatalf("max: got %d, expected %d", ef.Max(), Max(buf.Bytes()))
		}
		if ef.Min() != Min(buf.Bytes()) {
			t.Fatalf("min: got %d, expected %d", ef.Min(), Min(buf.Bytes()))
		}
		if ef.Count() != Count(buf.Bytes()) {
			t.Fatalf("max: got %d, expected %d", ef.Count(), Count(buf.Bytes()))
		}
	})
}

func FuzzEliasFanoAddOffset(f *testing.F) {
	f.Fuzz(func(t *testing.T, in []byte) {
		if len(in) < 2 {
			t.Skip()
		}
		for len(in) < int(2*superQ) {
			in = append(in, in...)
		}

		count := len(in)
		keys := make([]uint64, count+1)
		for i, b := range in {
			keys[i+1] = keys[i] + uint64(b)
		}
		maxVal := keys[count]

		// Build with fresh allocation
		ef1 := NewEliasFano(uint64(count+1), maxVal)
		for _, c := range keys {
			ef1.AddOffset(c)
		}
		ef1.Build()

		// Build with ResetForWrite (reuse path) — first build a large EF
		// with non-zero data, then reset and rebuild to verify clear works
		shiftedMax := maxVal + uint64(count)
		ef2 := NewEliasFano(uint64(count+1), shiftedMax)
		for i, c := range keys {
			ef2.AddOffset(c + uint64(i))
		}
		ef2.Build()
		ef2.ResetForWrite(uint64(count+1), maxVal)
		for _, c := range keys {
			ef2.AddOffset(c)
		}
		ef2.Build()

		// Verify both produce identical Get results
		for i := range keys {
			v1 := ef1.Get(uint64(i))
			v2 := ef2.Get(uint64(i))
			if v1 != keys[i] {
				t.Fatalf("ef1.Get(%d) = %d, want %d", i, v1, keys[i])
			}
			if v2 != keys[i] {
				t.Fatalf("ef2.Get(%d) = %d, want %d", i, v2, keys[i])
			}
		}

		// Verify Get2 (pair retrieval)
		for i := 0; i < count; i++ {
			v, vNext := ef1.Get2(uint64(i))
			if v != keys[i] {
				t.Fatalf("Get2(%d) val = %d, want %d", i, v, keys[i])
			}
			if vNext != keys[i+1] {
				t.Fatalf("Get2(%d) next = %d, want %d", i, vNext, keys[i+1])
			}
		}

		// Verify Seek finds correct values
		for i := range keys {
			target := keys[i]
			found, ok := ef1.Seek(target)
			if !ok {
				t.Fatalf("Seek(%d) not found", target)
			}
			if found != target {
				t.Fatalf("Seek(%d) = %d, want exact match", target, found)
			}
		}

		// Seek beyond max returns not-found
		if _, ok := ef1.Seek(maxVal + 1); ok {
			t.Fatalf("Seek(%d) should not find anything beyond max", maxVal+1)
		}

		// Seek for value between elements finds next
		if count > 1 && keys[1] > keys[0]+1 {
			mid := keys[0] + 1
			found, ok := ef1.Seek(mid)
			if !ok {
				t.Fatalf("Seek(%d) not found", mid)
			}
			if found < mid {
				t.Fatalf("Seek(%d) = %d, should be >= target", mid, found)
			}
		}

		// Verify serialization round-trip preserves data
		buf := ef1.AppendBytes(nil)
		efRead, _ := ReadEliasFano(buf)
		for i := range keys {
			if efRead.Get(uint64(i)) != keys[i] {
				t.Fatalf("round-trip Get(%d) = %d, want %d", i, efRead.Get(uint64(i)), keys[i])
			}
		}
	})
}

func FuzzDoubleEliasFano(f *testing.F) {
	f.Fuzz(func(t *testing.T, in []byte) {
		if len(in)%2 == 1 {
			t.Skip()
		}
		if len(in) == 0 {
			t.Skip()
		}
		for len(in) < int(2*superQ) { // make input large enough to trigger supreQ jump logic
			in = append(in, in...)
		}

		var ef DoubleEliasFano
		// Treat each byte of the sequence as difference between previous value and the next
		numBuckets := len(in) / 2
		cumKeys := make([]uint64, numBuckets+1)
		position := make([]uint64, numBuckets+1)
		for i, b := range in[:numBuckets] {
			cumKeys[i+1] = cumKeys[i] + uint64(b)
		}
		for i, b := range in[numBuckets:] {
			position[i+1] = position[i] + uint64(b)
		}
		ef1 := NewEliasFano(uint64(numBuckets+1), cumKeys[numBuckets])
		for _, c := range cumKeys {
			ef1.AddOffset(c)
		}
		ef1.Build()
		ef2 := NewEliasFano(uint64(numBuckets+1), position[numBuckets])
		for _, p := range position {
			ef2.AddOffset(p)
		}
		ef2.Build()
		ef.Build(cumKeys, position)
		// Try to read from ef
		for bucket := 0; bucket < numBuckets; bucket++ {
			cumKey, bitPos := ef.Get2(uint64(bucket))
			if cumKey != cumKeys[bucket] {
				t.Fatalf("bucket %d: cumKey from EF = %d, expected %d", bucket, cumKey, cumKeys[bucket])
			}
			if bitPos != position[bucket] {
				t.Fatalf("bucket %d: position from EF = %d, expected %d", bucket, bitPos, position[bucket])
			}
			cumKey = ef1.Get(uint64(bucket))
			if cumKey != cumKeys[bucket] {
				t.Fatalf("bucket %d: cumKey from EF1 = %d, expected %d", bucket, cumKey, cumKeys[bucket])
			}
			bitPos = ef2.Get(uint64(bucket))
			if bitPos != position[bucket] {
				t.Fatalf("bucket %d: position from EF2 = %d, expected %d", bucket, bitPos, position[bucket])
			}
		}
		for bucket := 0; bucket < numBuckets; bucket++ {
			cumKey, cumKeysNext, bitPos := ef.Get3(uint64(bucket))
			if cumKey != cumKeys[bucket] {
				t.Fatalf("bucket %d: cumKey from EF = %d, expected %d", bucket, cumKey, cumKeys[bucket])
			}
			if bitPos != position[bucket] {
				t.Fatalf("bucket %d: position from EF = %d, expected %d", bucket, bitPos, position[bucket])
			}
			if cumKeysNext != cumKeys[bucket+1] {
				t.Fatalf("bucket %d: cumKeysNext from EF = %d, expected %d", bucket, cumKeysNext, cumKeys[bucket+1])
			}
		}
	})
}
