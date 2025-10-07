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

package eliasfano16

import (
	"testing"
)

// go test -trimpath -v -fuzz=FuzzEliasFano -fuzztime=10s ./recsplit

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
		var minDeltaCumKeys uint64
		for i, b := range in {
			keys[i+1] = keys[i] + uint64(b)
			if i == 0 || uint64(b) < minDeltaCumKeys {
				minDeltaCumKeys = uint64(b)
			}
		}
		ef := NewEliasFano(uint64(count+1), keys[count], minDeltaCumKeys)
		for _, c := range keys {
			ef.AddOffset(c)
		}
		ef.Build()

		// Try to read from ef
		for i := 0; i < count; i++ {
			if ef.Get(uint64(i)) != keys[i] {
				t.Fatalf("i %d: got %d, expected %d", i, ef.Get(uint64(i)), keys[i])
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
		var minDeltaCumKeys, minDeltaPosition uint64
		position := make([]uint64, numBuckets+1)
		for i, b := range in[:numBuckets] {
			cumKeys[i+1] = cumKeys[i] + uint64(b)
			if i == 0 || uint64(b) < minDeltaCumKeys {
				minDeltaCumKeys = uint64(b)
			}
		}
		for i, b := range in[numBuckets:] {
			position[i+1] = position[i] + uint64(b)
			if i == 0 || uint64(b) < minDeltaPosition {
				minDeltaPosition = uint64(b)
			}
		}
		ef1 := NewEliasFano(uint64(numBuckets+1), cumKeys[numBuckets], minDeltaCumKeys)
		for _, c := range cumKeys {
			ef1.AddOffset(c)
		}
		ef1.Build()
		ef2 := NewEliasFano(uint64(numBuckets+1), position[numBuckets], minDeltaPosition)
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
