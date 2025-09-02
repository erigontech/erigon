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

package eth2shuffle

import "encoding/binary"

type HashFn func(input []byte) []byte

const hSeedSize = int8(32)
const hRoundSize = int8(1)
const hPositionWindowSize = int8(4)
const hPivotViewSize = hSeedSize + hRoundSize
const hTotalSize = hSeedSize + hRoundSize + hPositionWindowSize

// To make it completely clear:
// Memory layout hash input:
// |                       32 bytes for seed     ...          || 1 byte, round || 4 bytes for position window  ...|
// |    <---------------------- for pivot hash  -----     ...    -------->     ||       ignored for pivot hash    |
// |    <---------------------------------------- for source hash --------------------------------------------->  |

/*
Return `p(index)` in a pseudorandom permutation `p` of `0...list_size-1` with ``seed`` as entropy.

    Utilizes 'swap or not' shuffling found in
    https://link.springer.com/content/pdf/10.1007%2F978-3-642-32009-5_1.pdf
    See the 'generalized domain' algorithm on page 3.

Eth 2.0 spec implementation here:
	https://github.com/ethereum/eth2.0-specs/blob/dev/specs/core/0_beacon-chain.md#get_permuted_index
*/

// Permute index, i.e. shuffle an individual list item without allocating a complete list.
// Returns the index in the would-be shuffled list.
func PermuteIndex(hashFn HashFn, rounds uint8, index uint64, listSize uint64, seed [32]byte) uint64 {
	return innerPermuteIndex(hashFn, rounds, index, listSize, seed, true)
}

// Inverse of PermuteIndex, returns original index when given the same shuffling context parameters and permuted index.
func UnpermuteIndex(hashFn HashFn, rounds uint8, index uint64, listSize uint64, seed [32]byte) uint64 {
	return innerPermuteIndex(hashFn, rounds, index, listSize, seed, false)
}

func innerPermuteIndex(hashFn HashFn, rounds uint8, index uint64, listSize uint64, seed [32]byte, dir bool) uint64 {
	if rounds == 0 {
		return index
	}
	buf := make([]byte, hTotalSize, hTotalSize)
	r := uint8(0)
	if !dir {
		// Start at last round.
		// Iterating through the rounds in reverse, un-swaps everything, effectively un-shuffling the list.
		r = rounds - 1
	}
	// Seed is always the first 32 bytes of the hash input, we never have to change this part of the buffer.
	copy(buf[:hSeedSize], seed[:])
	for {
		// spec: pivot = bytes_to_int(hash(seed + int_to_bytes1(round))[0:8]) % list_size
		// This is the "int_to_bytes1(round)", appended to the seed.
		buf[hSeedSize] = r
		// Seed is already in place, now just hash the correct part of the buffer, and take a uint64 from it,
		//  and modulo it to get a pivot within range.
		pivot := binary.LittleEndian.Uint64(hashFn(buf[:hPivotViewSize])[:8]) % listSize
		// spec: flip = (pivot - index) % list_size
		// Add extra list_size to prevent underflows.
		// "flip" will be the other side of the pair
		flip := (pivot + (listSize - index)) % listSize
		// spec: position = max(index, flip)
		// Why? Don't do double work: we consider every pair only once.
		// (Otherwise we would swap it back in place)
		// Pick the highest index of the pair as position to retrieve randomness with.
		position := index
		if flip > position {
			position = flip
		}
		// spec: source = hash(seed + int_to_bytes1(round) + int_to_bytes4(position // 256))
		// - seed is still in 0:32 (excl., 32 bytes)
		// - round number is still in 32
		// - mix in the position for randomness, except the last byte of it,
		//     which will be used later to select a bit from the resulting hash.
		binary.LittleEndian.PutUint32(buf[hPivotViewSize:], uint32(position>>8))
		source := hashFn(buf)
		// spec: byte = source[(position % 256) // 8]
		// Effectively keep the first 5 bits of the byte value of the position,
		//  and use it to retrieve one of the 32 (= 2^5) bytes of the hash.
		byteV := source[(position&0xff)>>3]
		// Using the last 3 bits of the position-byte, determine which bit to get from the hash-byte (8 bits, = 2^3)
		// spec: bit = (byte >> (position % 8)) % 2
		bitV := (byteV >> (position & 0x7)) & 0x1
		// Now that we have our "coin-flip", swap index, or don't.
		// If bitV, flip.
		if bitV == 1 {
			index = flip
		}
		// go forwards?
		if dir {
			// -> shuffle
			r++
			if r == rounds {
				break
			}
		} else {
			if r == 0 {
				break
			}
			// -> un-shuffle
			r--
		}
	}
	return index
}

/*

def shuffle(list_size, seed):
    indices = list(range(list_size))
    for round in range(90):
        hash_bytes = b''.join([
            hash(seed + round.to_bytes(1, 'little') + (i).to_bytes(4, 'little'))
            for i in range((list_size + 255) // 256)
        ])
        pivot = int.from_bytes(hash(seed + round.to_bytes(1, 'little')), 'little') % list_size

        powers_of_two = [1, 2, 4, 8, 16, 32, 64, 128]

        for i, index in enumerate(indices):
            flip = (pivot - index) % list_size
            hash_pos = index if index > flip else flip
            byte = hash_bytes[hash_pos // 8]
            if byte & powers_of_two[hash_pos % 8]:
                indices[i] = flip
    return indices

Heavily-optimized version of the set-shuffling algorithm proposed by Vitalik to shuffle all items in a list together.

Original here:
	https://github.com/ethereum/eth2.0-specs/pull/576#issue-250741806

Main differences, implemented by @protolambda:
    - User can supply input slice to shuffle, simple provide [0,1,2,3,4, ...] to get a list of cleanly shuffled indices.
    - Input slice is shuffled (hence no return value), no new array is allocated
    - Allocations as minimal as possible: only a very minimal buffer for hashing
	  (this should be allocated on the stack, compiler will find it with escape analysis).
		This is not bigger than what's used for shuffling a single index!
		As opposed to larger allocations (size O(n) instead of O(1)) made in the original.
    - Replaced pseudocode/python workarounds with bit-logic.
    - User can provide their own hash-function (as long as it outputs a 32 len byte slice)

*/

// Shuffles the list
func ShuffleList[T any](hashFn HashFn, input []T, rounds uint8, seed [32]byte) {
	innerShuffleList(hashFn, input, rounds, seed, true)
}

// Un-shuffles the list
func UnshuffleList[T any](hashFn HashFn, input []T, rounds uint8, seed [32]byte) {
	innerShuffleList(hashFn, input, rounds, seed, false)
}

// Shuffles or unshuffles, depending on the `dir` (true for shuffling, false for unshuffling
func innerShuffleList[T any](hashFn HashFn, input []T, rounds uint8, seed [32]byte, dir bool) {
	if len(input) <= 1 {
		// nothing to (un)shuffle
		return
	}
	if rounds == 0 {
		return
	}
	listSize := uint64(len(input))
	buf := make([]byte, hTotalSize, hTotalSize)
	r := uint8(0)
	if !dir {
		// Start at last round.
		// Iterating through the rounds in reverse, un-swaps everything, effectively un-shuffling the list.
		r = rounds - 1
	}
	// Seed is always the first 32 bytes of the hash input, we never have to change this part of the buffer.
	copy(buf[:hSeedSize], seed[:])
	for {
		// spec: pivot = bytes_to_int(hash(seed + int_to_bytes1(round))[0:8]) % list_size
		// This is the "int_to_bytes1(round)", appended to the seed.
		buf[hSeedSize] = r
		// Seed is already in place, now just hash the correct part of the buffer, and take a uint64 from it,
		//  and modulo it to get a pivot within range.
		pivot := binary.LittleEndian.Uint64(hashFn(buf[:hPivotViewSize])[:8]) % listSize

		// Split up the for-loop in two:
		//  1. Handle the part from 0 (incl) to pivot (incl). This is mirrored around (pivot / 2)
		//  2. Handle the part from pivot (excl) to N (excl). This is mirrored around ((pivot / 2) + (size/2))
		// The pivot defines a split in the array, with each of the splits mirroring their data within the split.
		// Print out some example even/odd sized index lists, with some even/odd pivots,
		//  and you can deduce how the mirroring works exactly.
		// Note that the mirror is strict enough to not consider swapping the index @mirror with itself.
		mirror := (pivot + 1) >> 1
		// Since we are iterating through the "positions" in order, we can just repeat the hash every 256th position.
		// No need to pre-compute every possible hash for efficiency like in the example code.
		// We only need it consecutively (we are going through each in reverse order however, but same thing)
		//
		// spec: source = hash(seed + int_to_bytes1(round) + int_to_bytes4(position // 256))
		// - seed is still in 0:32 (excl., 32 bytes)
		// - round number is still in 32
		// - mix in the position for randomness, except the last byte of it,
		//     which will be used later to select a bit from the resulting hash.
		// We start from the pivot position, and work back to the mirror position (of the part left to the pivot).
		// This makes us process each pear exactly once (instead of unnecessarily twice, like in the spec)
		binary.LittleEndian.PutUint32(buf[hPivotViewSize:], uint32(pivot>>8))
		source := hashFn(buf)
		byteV := source[(pivot&0xff)>>3]
		for i, j := uint64(0), pivot; i < mirror; i, j = i+1, j-1 {
			// The pair is i,j. With j being the bigger of the two, hence the "position" identifier of the pair.
			// Every 256th bit (aligned to j).
			if j&0xff == 0xff {
				// just overwrite the last part of the buffer, reuse the start (seed, round)
				binary.LittleEndian.PutUint32(buf[hPivotViewSize:], uint32(j>>8))
				source = hashFn(buf)
			}
			// Same trick with byte retrieval. Only every 8th.
			if j&0x7 == 0x7 {
				byteV = source[(j&0xff)>>3]
			}
			bitV := (byteV >> (j & 0x7)) & 0x1

			if bitV == 1 {
				// swap the pair items
				input[i], input[j] = input[j], input[i]
			}
		}
		// Now repeat, but for the part after the pivot.
		mirror = (pivot + listSize + 1) >> 1
		end := listSize - 1
		// Again, seed and round input is in place, just update the position.
		// We start at the end, and work back to the mirror point.
		// This makes us process each pear exactly once (instead of unnecessarily twice, like in the spec)
		binary.LittleEndian.PutUint32(buf[hPivotViewSize:], uint32(end>>8))
		source = hashFn(buf)
		byteV = source[(end&0xff)>>3]
		for i, j := pivot+1, end; i < mirror; i, j = i+1, j-1 {
			// Exact same thing (copy of above loop body)
			//--------------------------------------------
			// The pair is i,j. With j being the bigger of the two, hence the "position" identifier of the pair.
			// Every 256th bit (aligned to j).
			if j&0xff == 0xff {
				// just overwrite the last part of the buffer, reuse the start (seed, round)
				binary.LittleEndian.PutUint32(buf[hPivotViewSize:], uint32(j>>8))
				source = hashFn(buf)
			}
			// Same trick with byte retrieval. Only every 8th.
			if j&0x7 == 0x7 {
				byteV = source[(j&0xff)>>3]
			}
			bitV := (byteV >> (j & 0x7)) & 0x1

			if bitV == 1 {
				// swap the pair items
				input[i], input[j] = input[j], input[i]
			}
			//--------------------------------------------
		}
		// go forwards?
		if dir {
			// -> shuffle
			r++
			if r == rounds {
				break
			}
		} else {
			if r == 0 {
				break
			}
			// -> un-shuffle
			r--
		}
	}
}
