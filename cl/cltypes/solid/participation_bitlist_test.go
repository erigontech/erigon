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

package solid_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestParticipationBitListStatic(t *testing.T) {
	require := require.New(t)

	bitList := solid.ParticipationBitList{}
	isStatic := bitList.Static()

	require.False(isStatic, "BitList Static method did not return false")
}

func TestParticipationBitListClear(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(10, 20)
	bitList.Clear()

	require.Zero(bitList.Length(), "BitList Clear did not reset the length to zero")
}

func TestParticipationBitListCopyTo(t *testing.T) {
	require := require.New(t)

	source := solid.NewParticipationBitList(5, 10)
	target := solid.NewParticipationBitList(0, 0)

	source.CopyTo(target)

	expectedHash, err := source.HashSSZ()
	require.NoError(err)

	actualHash, err := target.HashSSZ()
	require.NoError(err)

	require.Equal(expectedHash, actualHash, "BitList CopyTo did not copy the bits correctly")
	require.Equal(source.Length(), target.Length(), "BitList CopyTo did not set the target length correctly")
}

func TestParticipationBitListPop(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	popped := bitList.Pop()

	require.Equal(byte(0), popped, "BitList Pop did not return the expected value")
	require.Equal(4, bitList.Length(), "BitList Pop did not decrement the length")
}

func TestParticipationBitListAppend(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	bitList.Append(1)
	bitList.Append(0)

	require.Equal(7, bitList.Length(), "BitList Append did not increment the length correctly")
	require.Equal(byte(1), bitList.Get(5), "BitList Append did not append the bits correctly")
	require.Equal(byte(0), bitList.Get(6), "BitList Append did not append the bits correctly")
}

func TestParticipationBitListGet(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	bit := bitList.Get(2)

	require.Zero(bit, "BitList Get did not return the expected value")
}

func TestParticipationBitListSet(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	bitList.Set(2, 1)

	require.Equal(byte(1), bitList.Get(2), "BitList Set did not set the bit correctly")
}

func TestParticipationBitListLength(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	length := bitList.Length()

	require.Equal(5, length, "BitList Length did not return the expected value")
}

func TestParticipationBitListCap(t *testing.T) {
	require := require.New(t)

	bitList := solid.NewParticipationBitList(5, 10)

	capacity := bitList.Cap()

	require.Equal(10, capacity, "BitList Cap did not return the expected value")
}

// Add more tests as needed for other functions in the BitList struct.

// sha256ListRoot computes hash_tree_root(List[uint8, limit]) with crypto/sha256 only.
func sha256ListRoot(data []byte, limit int) [32]byte {
	layer := make([][32]byte, 0, (len(data)+31)/32+1)
	for i := 0; i < len(data); i += 32 {
		var chunk [32]byte
		copy(chunk[:], data[i:min(len(data), i+32)])
		layer = append(layer, chunk)
	}
	var zero [32]byte
	for depth := 0; 1<<depth < (limit+31)/32; depth++ {
		if len(layer)%2 == 1 {
			layer = append(layer, zero)
		}
		for i := range len(layer) / 2 {
			layer[i] = sha256.Sum256(append(layer[2*i][:], layer[2*i+1][:]...))
		}
		layer = layer[:len(layer)/2]
		zero = sha256.Sum256(append(zero[:], zero[:]...))
	}
	root := zero
	if len(layer) > 0 {
		root = layer[0]
	}
	var length [32]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(data)))
	return sha256.Sum256(append(root[:], length[:]...))
}

func randomParticipation(rng *rand.Rand, n int) []byte {
	values := make([]byte, n)
	for i := range values {
		values[i] = byte(1 + rng.Intn(7))
	}
	return values
}

func appendAll(bitList *solid.ParticipationBitList, values []byte) *solid.ParticipationBitList {
	for _, v := range values {
		bitList.Append(v)
	}
	return bitList
}

func hashedBitList(t *testing.T, values []byte, limit int) *solid.ParticipationBitList {
	t.Helper()
	bitList := appendAll(solid.NewParticipationBitList(0, limit), values)
	_, err := bitList.HashSSZ()
	require.NoError(t, err)
	return bitList
}

func TestParticipationBitListHashSSZMatchesSha256Oracle(t *testing.T) {
	const subtreeBytes = solid.ParticipationSubtreeChunks * 32
	limit := int(clparams.MainnetBeaconConfig.ValidatorRegistryLimit)
	rng := rand.New(rand.NewSource(1))
	producers := []struct {
		name    string
		produce func(t *testing.T, values []byte) *solid.ParticipationBitList
	}{
		{"new_and_set", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			bitList := solid.NewParticipationBitList(len(values), limit)
			for i, v := range values {
				bitList.Set(i, v)
			}
			return bitList
		}},
		{"set_past_length", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			bitList := solid.NewParticipationBitList(len(values), limit)
			for i, v := range values {
				bitList.Set(i, v)
			}
			bitList.Set(len(values), 7)
			return bitList
		}},
		{"append", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			return appendAll(solid.NewParticipationBitList(0, limit), values)
		}},
		{"decode_ssz", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			bitList := solid.NewParticipationBitList(0, limit)
			require.NoError(t, bitList.DecodeSSZ(values, 0))
			return bitList
		}},
		{"copy", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			return hashedBitList(t, values, limit).Copy()
		}},
		{"copy_to", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			target := solid.NewParticipationBitList(0, limit)
			hashedBitList(t, values, limit).CopyTo(target)
			return target
		}},
		{"clear_then_shorter_append", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			bitList := hashedBitList(t, bytes.Repeat([]byte{0xff}, 2*len(values)+subtreeBytes), limit)
			bitList.Clear()
			return appendAll(bitList, values)
		}},
		{"json_reuse_shorter", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			bitList := hashedBitList(t, bytes.Repeat([]byte{0xff}, len(values)+100), limit)
			list := make([]string, len(values))
			for i, v := range values {
				list[i] = strconv.Itoa(int(v))
			}
			input, err := json.Marshal(list)
			require.NoError(t, err)
			require.NoError(t, bitList.UnmarshalJSON(input))
			return bitList
		}},
		{"rehash_after_set", func(t *testing.T, values []byte) *solid.ParticipationBitList {
			bitList := hashedBitList(t, bytes.Repeat([]byte{0xff}, len(values)), limit)
			for i, v := range values {
				bitList.Set(i, v)
			}
			return bitList
		}},
	}
	for _, length := range []int{1, 31, 33, subtreeBytes, 2*subtreeBytes + 65, 3*subtreeBytes - 1, 5 * subtreeBytes, 2_100_001} {
		values := randomParticipation(rng, length)
		want := sha256ListRoot(values, limit)
		for _, producer := range producers {
			t.Run(fmt.Sprintf("%s/%d", producer.name, length), func(t *testing.T) {
				bitList := producer.produce(t, values)
				require.Equal(t, values, bitList.Bytes())
				root, err := bitList.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, want, root)
			})
		}
	}
}

func TestParticipationBitListHashSSZSmallCapacity(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for _, limit := range []int{1, 32, 33, 64, 1000, solid.ParticipationSubtreeChunks*32 + 1} {
		for _, length := range []int{1, limit / 2, limit} {
			if length == 0 {
				continue
			}
			values := randomParticipation(rng, length)
			root, err := appendAll(solid.NewParticipationBitList(0, limit), values).HashSSZ()
			require.NoError(t, err)
			require.Equal(t, sha256ListRoot(values, limit), root, "limit %d length %d", limit, length)
		}
	}
}
