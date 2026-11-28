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

package solid

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const shareTestLimit = 1 << 20

func randomValidators(rng *rand.Rand, n int) []Validator {
	vals := make([]Validator, n)
	for i := range vals {
		vals[i] = randomValidatorBytes(rng)
	}
	return vals
}

func liveSetOf(vals []Validator, progressive bool) *liveSet {
	live := newLiveSet(shareTestLimit, progressive)
	for _, v := range vals {
		live.set.Append(v)
		live.model.append(v)
	}
	return live
}

func decodedLiveSetOf(t testing.TB, vals []Validator, progressive bool) *liveSet {
	t.Helper()
	live := newLiveSet(shareTestLimit, progressive)
	for _, v := range vals {
		live.model.append(v)
	}
	require.NoError(t, live.set.DecodeSSZ(live.model.encode(), 0))
	return live
}

func withReplaced(rng *rand.Rand, vals []Validator, indexes []int) []Validator {
	out := append([]Validator(nil), vals...)
	for _, i := range indexes {
		out[i] = randomValidatorBytes(rng)
	}
	return out
}

// expectedShared counts the chunks that are full in both lists and hold no differing element.
func expectedShared(layout cowLayout, nRecv, nRef int, differing []int) int {
	per := layout.chunkLeaves()
	shared := 0
	for ci := 0; (ci+1)*per-layout.shift <= min(nRecv, nRef); ci++ {
		lo, hi := ci*per-layout.shift, (ci+1)*per-layout.shift
		clean := true
		for _, d := range differing {
			clean = clean && (d < lo || d >= hi)
		}
		if clean {
			shared++
		}
	}
	return shared
}

func sharedChunkPointers(a, b *ValidatorSet) int {
	count := 0
	for ci := range min(len(a.list.chunks), len(b.list.chunks)) {
		if a.list.chunks[ci].Load() == b.list.chunks[ci].Load() {
			count++
		}
	}
	return count
}

func TestValidatorSetShareChunks(t *testing.T) {
	cases := []struct {
		name      string
		nRecv     int
		nRef      int
		differing []int
	}{
		{"identical", 3000, 3000, nil},
		{"exactly full chunks", 1024, 1024, nil},
		{"one chunk differs", 3000, 3000, []int{1300}},
		{"first chunk differs", 3000, 3000, []int{0}},
		{"partial last chunk differs", 3000, 3000, []int{2990}},
		{"every chunk differs", 3000, 3000, []int{10, 300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 2999}},
		{"reference longer", 3000, 4100, nil},
		{"reference longer, differing", 3000, 4100, []int{1000, 2999}},
		{"reference shorter", 3000, 1500, nil},
		{"reference shorter, differing", 3000, 1500, []int{200}},
		{"reference has one chunk worth", 3000, 84, nil},
		{"receiver shorter than first progressive chunk", 84, 3000, nil},
		{"both empty", 0, 0, nil},
	}
	for _, tc := range cases {
		for _, progressive := range []bool{false, true} {
			for _, hashedFirst := range []bool{false, true} {
				name := tc.name
				if progressive {
					name += "/progressive"
				}
				if hashedFirst {
					name += "/receiver hashed before share"
				}
				t.Run(name, func(t *testing.T) {
					rng := rand.New(rand.NewSource(int64(tc.nRecv*31 + tc.nRef)))
					refVals := randomValidators(rng, tc.nRef)
					recvVals := append([]Validator(nil), refVals[:min(tc.nRef, tc.nRecv)]...)
					recvVals = append(recvVals, randomValidators(rng, tc.nRecv-len(recvVals))...)
					for _, d := range tc.differing {
						recvVals[d] = randomValidatorBytes(rng)
					}

					ref := liveSetOf(refVals, progressive)
					_, err := ref.hash()
					require.NoError(t, err)
					recv := decodedLiveSetOf(t, recvVals, progressive)
					if hashedFirst {
						_, err := recv.hash()
						require.NoError(t, err)
					}
					refPointers := make([]*cowChunk, len(ref.set.list.chunks))
					for ci := range refPointers {
						refPointers[ci] = ref.set.list.chunks[ci].Load()
					}

					shared := recv.set.ShareChunks(ref.set)

					want := expectedShared(recv.set.list.layout, tc.nRecv, tc.nRef, tc.differing)
					require.Equal(t, want, shared)
					require.Equal(t, want, sharedChunkPointers(recv.set, ref.set))
					for ci, ch := range refPointers {
						require.Same(t, ch, ref.set.list.chunks[ci].Load(), "reference chunk %d replaced", ci)
					}
					ref.check(t, "reference after share")
					recv.check(t, "receiver after share")

					rng = rand.New(rand.NewSource(7))
					for range 3 {
						for _, i := range scatteredIndexes(max(tc.nRecv, 2)) {
							if i < recv.set.Length() && rng.Intn(2) == 0 {
								recv.setRandomAt(rng, i)
							}
							if i < ref.set.Length() && rng.Intn(2) == 0 {
								ref.setRandomAt(rng, i)
							}
						}
						recv.appendRandom(rng)
						ref.appendRandom(rng)
						recv.check(t, "receiver after writes")
						ref.check(t, "reference after writes")
					}
				})
			}
		}
	}
}

func TestValidatorSetShareChunksNeedsSameLayout(t *testing.T) {
	vals := make([]Validator, 3000)
	for i := range vals {
		vals[i] = NewValidator()
	}
	for _, refProgressive := range []bool{false, true} {
		ref := liveSetOf(vals, refProgressive)
		_, err := ref.hash()
		require.NoError(t, err)
		recv := decodedLiveSetOf(t, vals, !refProgressive)

		require.Zero(t, recv.set.ShareChunks(ref.set))

		require.Zero(t, sharedChunkPointers(recv.set, ref.set))
		ref.check(t, "reference")
		recv.check(t, "receiver")
	}
}

func TestValidatorSetShareChunksReleasesDecodeSlab(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	vals := randomValidators(rng, 3000)
	forBothLayouts(t, func(t *testing.T, progressive bool) {
		ref := liveSetOf(vals, progressive)
		recv := decodedLiveSetOf(t, withReplaced(rng, vals, []int{1300}), progressive)
		before := make([][]byte, len(recv.set.list.chunks))
		for ci := range before {
			before[ci] = recv.set.list.chunks[ci].Load().data
		}

		require.Positive(t, recv.set.ShareChunks(ref.set))

		unshared := 0
		for ci := range before {
			ch := recv.set.list.chunks[ci].Load()
			if ch == ref.set.list.chunks[ci].Load() {
				continue
			}
			unshared++
			require.NotSame(t, &before[ci][0], &ch.data[0], "chunk %d still points into the decode slab", ci)
			require.True(t, bytes.Equal(before[ci], ch.data), "chunk %d bytes changed", ci)
		}
		require.Positive(t, unshared)
	})
}

func TestValidatorSetShareChunksWithoutSharingKeepsSlab(t *testing.T) {
	rng := rand.New(rand.NewSource(6))
	forBothLayouts(t, func(t *testing.T, progressive bool) {
		ref := liveSetOf(randomValidators(rng, 3000), progressive)
		recv := decodedLiveSetOf(t, randomValidators(rng, 3000), progressive)
		first := recv.set.list.chunks[0].Load().data

		require.Zero(t, recv.set.ShareChunks(ref.set))

		require.Same(t, &first[0], &recv.set.list.chunks[0].Load().data[0])
	})
}

func TestValidatorSetSamePubkeys(t *testing.T) {
	rng := rand.New(rand.NewSource(9))
	const n = 3000
	vals := randomValidators(rng, n)
	withPubkey := func(i int) []Validator {
		out := append([]Validator(nil), vals...)
		v := Validator(bytes.Clone(out[i]))
		v.SetPublicKey([48]byte{0xff, byte(i)})
		out[i] = v
		return out
	}
	withBalance := func(i int) []Validator {
		out := append([]Validator(nil), vals...)
		v := Validator(bytes.Clone(out[i]))
		v.SetEffectiveBalance(12345)
		out[i] = v
		return out
	}

	forBothLayouts(t, func(t *testing.T, progressive bool) {
		ref := liveSetOf(vals, progressive)
		check := func(name string, recvVals []Validator, share bool, k int, want bool) {
			t.Run(name, func(t *testing.T) {
				recv := decodedLiveSetOf(t, recvVals, progressive)
				if share {
					recv.set.ShareChunks(ref.set)
				}
				require.Equal(t, want, recv.set.SamePubkeys(ref.set, k))
			})
		}
		check("equal", vals, false, n, true)
		check("equal after share", vals, true, n, true)
		check("equal prefix", vals, false, 1000, true)
		check("empty prefix", withPubkey(5), false, 0, true)
		check("other fields differ", withBalance(1300), false, n, true)
		check("other fields differ after share", withBalance(1300), true, n, true)
		check("pubkey differs in a full chunk", withPubkey(1300), false, n, false)
		check("pubkey differs in a full chunk after share", withPubkey(1300), true, n, false)
		check("pubkey differs in the partial chunk", withPubkey(n-1), false, n, false)
		check("pubkey differs in the partial chunk after share", withPubkey(n-1), true, n, false)
		check("pubkey differs past the prefix", withPubkey(1300), false, 1300, true)
		check("pubkey differs at the last prefix element", withPubkey(1299), false, 1300, false)
		check("k beyond the receiver", vals[:2000], false, 2001, false)
		check("k beyond the reference", vals, false, n+1, false)
		check("k at the receiver length", vals[:2000], false, 2000, true)

		other := decodedLiveSetOf(t, vals, !progressive)
		require.True(t, other.set.SamePubkeys(ref.set, n), "layouts differ but pubkeys are equal")
	})
}
