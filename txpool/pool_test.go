/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSubPoolMarkerOrder(t *testing.T) {
	require := require.New(t)
	require.Less(
		NewSubPoolMarker(true, true, true, true, false),
		NewSubPoolMarker(true, true, true, true, true),
	)
	require.Less(
		NewSubPoolMarker(true, true, true, false, true),
		NewSubPoolMarker(true, true, true, true, true),
	)
	require.Less(
		NewSubPoolMarker(true, true, true, false, true),
		NewSubPoolMarker(true, true, true, true, false),
	)
	require.Less(
		NewSubPoolMarker(false, true, true, true, true),
		NewSubPoolMarker(true, false, true, true, true),
	)
	require.Less(
		NewSubPoolMarker(false, false, false, true, true),
		NewSubPoolMarker(false, false, true, true, true),
	)
	require.Less(
		NewSubPoolMarker(false, false, true, true, false),
		NewSubPoolMarker(false, false, true, true, true),
	)
}

/*
func TestSubPoolOrder(t *testing.T) {
	sub := NewSubPool()
	sub.Add(&MetaTx{SubPool: 0b10101})
	sub.Add(&MetaTx{SubPool: 0b11110})
	sub.Add(&MetaTx{SubPool: 0b11101})
	sub.Add(&MetaTx{SubPool: 0b10001})
	require.Equal(t, uint8(0b11110), uint8(sub.Best().SubPool))
	require.Equal(t, uint8(0b10001), uint8(sub.Worst().SubPool))

	require.Equal(t, uint8(sub.Best().SubPool), uint8(sub.PopBest().SubPool))
	require.Equal(t, uint8(sub.Worst().SubPool), uint8(sub.PopWorst().SubPool))

	sub = NewSubPool()
	sub.Add(&MetaTx{SubPool: 0b00001})
	sub.Add(&MetaTx{SubPool: 0b01110})
	sub.Add(&MetaTx{SubPool: 0b01101})
	sub.Add(&MetaTx{SubPool: 0b00101})
	require.Equal(t, uint8(0b00001), uint8(sub.Worst().SubPool))
	require.Equal(t, uint8(0b01110), uint8(sub.Best().SubPool))

	require.Equal(t, uint8(sub.Worst().SubPool), uint8(sub.PopWorst().SubPool))
	require.Equal(t, uint8(sub.Best().SubPool), uint8(sub.PopBest().SubPool))
}

func TestSubPoolsPromote(t *testing.T) {
	s1 := []uint8{0b11000, 0b101, 0b111}
	s2 := []uint8{0b11000, 0b101, 0b111}
	s3 := []uint8{0b11000, 0b101, 0b111}
	pending, baseFee, queued := NewSubPool(), NewSubPool(), NewSubPool()
	for _, i := range s1 {
		pending.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
	}
	for _, i := range s2 {
		baseFee.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
	}
	for _, i := range s3 {
		queued.Add(&MetaTx{SubPool: SubPoolMarker(i & 0b11111)})
	}
	PromoteStep(pending, baseFee, queued)

	if pending.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(pending.Worst().SubPool))
	}
	if baseFee.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(baseFee.Worst().SubPool))
	}
	if queued.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(queued.Worst().SubPool))
	}
	// if limit reached, worst must be greater than X
}

//nolint
func hexToSubPool(s string) []uint8 {
	a, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	for i := range a {
		a[i] &= 0b11111
	}

	return a
}
*/
