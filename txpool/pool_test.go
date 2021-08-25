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

/*
func TestSubPoolOrder(t *testing.T) {
	sub := NewSubPool()
	sub.OnNewTxs(&metaTx{subPool: 0b10101})
	sub.OnNewTxs(&metaTx{subPool: 0b11110})
	sub.OnNewTxs(&metaTx{subPool: 0b11101})
	sub.OnNewTxs(&metaTx{subPool: 0b10001})
	require.Equal(t, uint8(0b11110), uint8(sub.Best().subPool))
	require.Equal(t, uint8(0b10001), uint8(sub.Worst().subPool))

	require.Equal(t, uint8(sub.Best().subPool), uint8(sub.PopBest().subPool))
	require.Equal(t, uint8(sub.Worst().subPool), uint8(sub.PopWorst().subPool))

	sub = NewSubPool()
	sub.OnNewTxs(&metaTx{subPool: 0b00001})
	sub.OnNewTxs(&metaTx{subPool: 0b01110})
	sub.OnNewTxs(&metaTx{subPool: 0b01101})
	sub.OnNewTxs(&metaTx{subPool: 0b00101})
	require.Equal(t, uint8(0b00001), uint8(sub.Worst().subPool))
	require.Equal(t, uint8(0b01110), uint8(sub.Best().subPool))

	require.Equal(t, uint8(sub.Worst().subPool), uint8(sub.PopWorst().subPool))
	require.Equal(t, uint8(sub.Best().subPool), uint8(sub.PopBest().subPool))
}

func TestSubPoolsPromote(t *testing.T) {
	s1 := []uint8{0b11000, 0b101, 0b111}
	s2 := []uint8{0b11000, 0b101, 0b111}
	s3 := []uint8{0b11000, 0b101, 0b111}
	pending, baseFee, queued := NewSubPool(), NewSubPool(), NewSubPool()
	for _, i := range s1 {
		pending.OnNewTxs(&metaTx{subPool: SubPoolMarker(i & 0b11111)})
	}
	for _, i := range s2 {
		baseFee.OnNewTxs(&metaTx{subPool: SubPoolMarker(i & 0b11111)})
	}
	for _, i := range s3 {
		queued.OnNewTxs(&metaTx{subPool: SubPoolMarker(i & 0b11111)})
	}
	promote(pending, baseFee, queued)

	if pending.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(pending.Worst().subPool))
	}
	if baseFee.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(baseFee.Worst().subPool))
	}
	if queued.Worst() != nil {
		require.Less(t, uint8(0b01111), uint8(queued.Worst().subPool))
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
