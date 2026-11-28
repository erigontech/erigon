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
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// validatorModel is a cache-free reference registry: flat validators plus
// per-validator leaf roots, merkleized with the stock helpers.
type validatorModel struct {
	vals  []Validator
	roots [][32]byte
	limit int
}

func newValidatorModel(limit int) *validatorModel {
	return &validatorModel{limit: limit}
}

func validatorLeafRoot(v Validator) [32]byte {
	var hashBuffer [8 * 32]byte
	if err := v.CopyHashBufferTo(hashBuffer[:]); err != nil {
		panic(err)
	}
	var root [32]byte
	if err := merkle_tree.MerkleRootFromFlatLeaves(hashBuffer[:], root[:]); err != nil {
		panic(err)
	}
	return root
}

func (m *validatorModel) append(v Validator) {
	m.vals = append(m.vals, bytes.Clone(v))
	m.roots = append(m.roots, validatorLeafRoot(v))
}

func (m *validatorModel) set(i int, v Validator) {
	m.vals[i] = bytes.Clone(v)
	m.roots[i] = validatorLeafRoot(v)
}

func (m *validatorModel) mutate(i int, fn func(Validator)) {
	v := bytes.Clone(m.vals[i])
	fn(v)
	m.set(i, v)
}

func (m *validatorModel) clone() *validatorModel {
	c := &validatorModel{limit: m.limit, roots: append([][32]byte(nil), m.roots...)}
	for _, v := range m.vals {
		c.vals = append(c.vals, bytes.Clone(v))
	}
	return c
}

func (m *validatorModel) encode() []byte {
	out := make([]byte, 0, len(m.vals)*validatorSize)
	for _, v := range m.vals {
		out = append(out, v...)
	}
	return out
}

func (m *validatorModel) root(t testing.TB, progressive bool) [32]byte {
	t.Helper()
	lengthRoot := merkle_tree.Uint64Root(uint64(len(m.vals)))
	roots := slices.Clone(m.roots) // the merkleization helpers hash in place
	if progressive {
		root, err := merkle_tree.ProgressiveListRoot(roots, uint64(len(m.vals)))
		require.NoError(t, err)
		return root
	}
	root, err := merkle_tree.MerkleizeVector(roots, uint64(m.limit))
	require.NoError(t, err)
	return crypto.Sha256(root[:], lengthRoot[:])
}

func (m *validatorModel) truncated(n int) *validatorModel {
	c := &validatorModel{limit: m.limit, roots: append([][32]byte(nil), m.roots[:n]...)}
	for _, v := range m.vals[:n] {
		c.vals = append(c.vals, bytes.Clone(v))
	}
	return c
}

func randomValidatorBytes(rng *rand.Rand) Validator {
	v := make(Validator, validatorSize)
	if _, err := rng.Read(v); err != nil {
		panic(err)
	}
	return v
}

// liveSet pairs a registry under test with its reference model.
type liveSet struct {
	set         *ValidatorSet
	model       *validatorModel
	progressive bool
}

func newLiveSet(limit int, progressive bool) *liveSet {
	set := NewValidatorSet(limit)
	set.SetProgressiveHashing(progressive)
	return &liveSet{set: set, model: newValidatorModel(limit), progressive: progressive}
}

func (l *liveSet) hash() ([32]byte, error) {
	if l.progressive {
		return l.set.HashSSZProgressive()
	}
	return l.set.HashSSZ()
}

func requireSlotsCacheChunkData(t testing.TB, l *cowList, label string) {
	t.Helper()
	slots := l.chunks[:cap(l.chunks)]
	for i := range slots {
		var want *byte
		if ch := slots[i].Load(); ch != nil {
			want = unsafe.SliceData(ch.data)
		}
		require.True(t, want == slots[i].base.Load(), "%s: slot %d caches a stale data pointer", label, i)
	}
}

func (l *liveSet) check(t testing.TB, label string) {
	t.Helper()
	requireSlotsCacheChunkData(t, &l.set.list, label)
	encoded, err := l.set.EncodeSSZ(nil)
	require.NoError(t, err, label)
	require.True(t, bytes.Equal(l.model.encode(), encoded), "%s: encoding differs from the model", label)
	require.Equal(t, len(l.model.vals), l.set.Length(), label)
	got, err := l.hash()
	require.NoError(t, err, label)
	require.Equal(t, l.model.root(t, l.progressive), got, "%s: root differs from the oracle", label)
}

func (l *liveSet) appendRandom(rng *rand.Rand) {
	v := randomValidatorBytes(rng)
	l.set.Append(v)
	l.model.append(v)
}

func (l *liveSet) copyInto(dst *liveSet) {
	l.set.CopyTo(dst.set)
	dst.model = l.model.clone()
	dst.progressive = l.progressive
}

var validatorFieldSetters = []struct {
	name  string
	set   func(*ValidatorSet, int, uint64)
	apply func(Validator, uint64)
}{
	{
		"withdrawal credentials",
		func(s *ValidatorSet, i int, x uint64) {
			s.SetWithdrawalCredentialForValidatorAtIndex(i, hashFromUint64(x))
		},
		func(v Validator, x uint64) { v.SetWithdrawalCredentials(hashFromUint64(x)) },
	},
	{
		"exit epoch",
		(*ValidatorSet).SetExitEpochForValidatorAtIndex,
		Validator.SetExitEpoch,
	},
	{
		"withdrawable epoch",
		(*ValidatorSet).SetWithdrawableEpochForValidatorAtIndex,
		Validator.SetWithdrawableEpoch,
	},
	{
		"effective balance",
		(*ValidatorSet).SetEffectiveBalanceForValidatorAtIndex,
		Validator.SetEffectiveBalance,
	},
	{
		"activation epoch",
		(*ValidatorSet).SetActivationEpochForValidatorAtIndex,
		Validator.SetActivationEpoch,
	},
	{
		"activation eligibility epoch",
		(*ValidatorSet).SetActivationEligibilityEpochForValidatorAtIndex,
		Validator.SetActivationEligibilityEpoch,
	},
	{
		"slashed",
		func(s *ValidatorSet, i int, x uint64) { s.SetValidatorSlashed(i, x&1 == 1) },
		func(v Validator, x uint64) { v.SetSlashed(x&1 == 1) },
	},
}

func hashFromUint64(x uint64) common.Hash {
	var h common.Hash
	for i := range h {
		h[i] = byte(x >> (8 * (i % 8)))
		x = x*6364136223846793005 + 1442695040888963407
	}
	return h
}

func (l *liveSet) applyRandomSetter(rng *rand.Rand) {
	if len(l.model.vals) == 0 {
		return
	}
	setter := validatorFieldSetters[rng.Intn(len(validatorFieldSetters))]
	index := rng.Intn(len(l.model.vals))
	value := rng.Uint64()
	setter.set(l.set, index, value)
	l.model.mutate(index, func(v Validator) { setter.apply(v, value) })
}

func TestValidatorSetOracleDifferential(t *testing.T) {
	for _, progressive := range []bool{false, true} {
		for seed := int64(1); seed <= 3; seed++ {
			// With hashEvery > 1, copies and writes meet chunks and trees that are still dirty.
			for _, hashEvery := range []int{1, 7} {
				t.Run(fmt.Sprintf("progressive=%v/seed=%d/hashEvery=%d", progressive, seed, hashEvery), func(t *testing.T) {
					runOracleDifferential(t, rand.New(rand.NewSource(seed)), progressive, hashEvery)
				})
			}
		}
	}
}

func (l *liveSet) checkEncoding(t testing.TB, label string) {
	t.Helper()
	encoded, err := l.set.EncodeSSZ(nil)
	require.NoError(t, err, label)
	require.True(t, bytes.Equal(l.model.encode(), encoded), "%s: encoding differs from the model", label)
}

func runOracleDifferential(t *testing.T, rng *rand.Rand, progressive bool, hashEvery int) {
	const limit = 1 << 20
	pool := make([]*liveSet, 5)
	for i := range pool {
		pool[i] = newLiveSet(limit, progressive)
	}
	pick := func() *liveSet { return pool[rng.Intn(len(pool))] }
	for step := range 400 {
		l := pick()
		var op string
		switch rng.Intn(10) {
		case 0, 1:
			op = "append burst"
			for range 1 + rng.Intn(300) {
				l.appendRandom(rng)
			}
		case 2:
			op = "append one"
			l.appendRandom(rng)
		case 3:
			op = "set"
			if len(l.model.vals) > 0 {
				i := rng.Intn(len(l.model.vals))
				v := randomValidatorBytes(rng)
				l.set.Set(i, v)
				l.model.set(i, v)
			}
		case 4, 5:
			op = "field setter"
			for range 1 + rng.Intn(5) {
				l.applyRandomSetter(rng)
			}
		case 6, 7:
			op = "copy"
			dst := pick()
			if dst == l {
				dst = &liveSet{set: NewValidatorSet(limit), model: newValidatorModel(limit)}
				pool[rng.Intn(len(pool))] = dst
			}
			if rng.Intn(2) == 0 {
				_, err := l.hash()
				require.NoError(t, err)
			}
			l.copyInto(dst)
		case 8:
			op = "encode and decode"
			encoded, err := l.set.EncodeSSZ(nil)
			require.NoError(t, err)
			decoded := &liveSet{set: NewValidatorSet(limit), model: l.model.clone(), progressive: l.progressive}
			decoded.set.SetProgressiveHashing(l.progressive)
			require.NoError(t, decoded.set.DecodeSSZ(encoded, 0))
			pool[rng.Intn(len(pool))] = decoded
		case 9:
			op = "layout switch"
			l.progressive = !l.progressive
			l.set.SetProgressiveHashing(l.progressive)
		}
		for i, live := range pool {
			label := fmt.Sprintf("step %d (%s), set %d", step, op, i)
			if step%hashEvery == 0 {
				live.check(t, label)
			} else {
				live.checkEncoding(t, label)
			}
		}
	}
}

func TestValidatorSetSmallLimits(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, limit := range []int{0, 1, 2, 3, 4, 5, 12, 16, 17, 255, 256, 257, 300, 512, 513} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			live := newLiveSet(limit, false)
			live.check(t, "empty")
			for i := range limit {
				live.appendRandom(rng)
				if i < 8 || i%37 == 0 || i == limit-1 {
					live.check(t, fmt.Sprintf("length %d", i+1))
				}
			}
			if limit > 0 {
				live.applyRandomSetter(rng)
				live.check(t, "setter at the limit")
				dst := &liveSet{set: NewValidatorSet(limit), model: newValidatorModel(limit)}
				live.copyInto(dst)
				dst.applyRandomSetter(rng)
				dst.check(t, "copy")
				live.check(t, "source after copy")
			}
		})
	}
}

func boundaryLengths() []int {
	lengths := []int{0, 1, 255, 256, 257, 84, 85, 86, 340, 341, 342}
	for _, start := range []int{1, 5, 21, 85, 341, 1365, 5461, 21845, 87381} {
		lengths = append(lengths, start-1, start, start+1)
	}
	return lengths
}

func TestValidatorSetBoundaryLengths(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	lengths := boundaryLengths()
	maxLength := 0
	for _, n := range lengths {
		maxLength = max(maxLength, n)
	}
	base := newValidatorModel(1 << 20)
	for range maxLength + 2 {
		base.append(randomValidatorBytes(rng))
	}
	for _, progressive := range []bool{false, true} {
		for _, n := range lengths {
			t.Run(fmt.Sprintf("progressive=%v/%d", progressive, n), func(t *testing.T) {
				built := &liveSet{set: NewValidatorSet(1 << 20), model: base.truncated(n), progressive: progressive}
				built.set.SetProgressiveHashing(progressive)
				for _, v := range built.model.vals {
					built.set.Append(v)
				}
				decoded := &liveSet{set: NewValidatorSet(1 << 20), model: base.truncated(n), progressive: progressive}
				decoded.set.SetProgressiveHashing(progressive)
				require.NoError(t, decoded.set.DecodeSSZ(decoded.model.encode(), 0))
				for name, live := range map[string]*liveSet{"appended": built, "decoded": decoded} {
					live.check(t, name)
					if n > 0 {
						for _, i := range []int{0, n / 2, n - 1} {
							v := randomValidatorBytes(rng)
							live.set.Set(i, v)
							live.model.set(i, v)
						}
						live.check(t, name+" after sets")
					}
					live.set.Append(base.vals[n])
					live.model.append(base.vals[n])
					live.check(t, name+" after append")
					copied := &liveSet{set: NewValidatorSet(1 << 20), model: newValidatorModel(1 << 20)}
					live.copyInto(copied)
					copied.applyRandomSetter(rng)
					copied.appendRandom(rng)
					copied.check(t, name+" copy")
					live.check(t, name+" source after copy")
				}
			})
		}
	}
}

func TestValidatorSetCopyMatrix(t *testing.T) {
	lengths := []int{0, 1, 255, 256, 257, 600, 1400}
	rng := rand.New(rand.NewSource(13))
	for _, progressive := range []bool{false, true} {
		for _, srcLen := range lengths {
			for _, dstLen := range append([]int{-1}, lengths...) {
				for _, srcFirst := range []bool{true, false} {
					for _, state := range []string{"cold", "warm", "dirty"} {
						name := fmt.Sprintf("progressive=%v/src=%d/dst=%d/srcFirst=%v/%s", progressive, srcLen, dstLen, srcFirst, state)
						t.Run(name, func(t *testing.T) {
							src := newLiveSet(1<<20, progressive)
							for range srcLen {
								src.appendRandom(rng)
							}
							if state != "cold" {
								_, err := src.hash()
								require.NoError(t, err)
							}
							if state == "dirty" {
								src.applyRandomSetter(rng)
								src.appendRandom(rng)
							}
							dst := newLiveSet(1<<20, !progressive)
							if dstLen >= 0 {
								for range dstLen {
									dst.appendRandom(rng)
								}
								_, err := dst.hash()
								require.NoError(t, err)
							}
							src.copyInto(dst)
							if state != "dirty" {
								src.check(t, "source after copy")
								dst.check(t, "destination after copy")
							}
							require.Len(t, dst.set.list.chunks, len(src.set.list.chunks))
							for i := len(dst.set.list.chunks); i < cap(dst.set.list.chunks); i++ {
								require.Nil(t, dst.set.list.chunks[:cap(dst.set.list.chunks)][i].Load(), "tail slot %d", i)
							}

							first, second := src, dst
							if !srcFirst {
								first, second = dst, src
							}
							for _, side := range []*liveSet{first, second} {
								side.appendRandom(rng)
								side.applyRandomSetter(rng)
								src.check(t, "source after mutating one side")
								dst.check(t, "destination after mutating one side")
							}
						})
					}
				}
			}
		}
	}
}

func TestValidatorSetCopyDoesNotLeakIntoSpareTail(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	long := newLiveSet(1<<20, false)
	for range 3000 {
		long.appendRandom(rng)
	}
	_, err := long.hash()
	require.NoError(t, err)
	short := newLiveSet(1<<20, false)
	for range 10 {
		short.appendRandom(rng)
	}
	short.copyInto(long)
	long.check(t, "spare after shrinking copy")
	for range 600 {
		long.appendRandom(rng)
	}
	long.check(t, "spare grown again")
	short.check(t, "source")
}

func TestValidatorSetPhase0SixBitCopyDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(19))
	const count = 700
	bitSetters := []struct {
		set func(*ValidatorSet, int, bool)
		get func(*ValidatorSet, int) bool
	}{
		{(*ValidatorSet).SetIsCurrentMatchingSourceAttester, (*ValidatorSet).IsCurrentMatchingSourceAttester},
		{(*ValidatorSet).SetIsPreviousMatchingSourceAttester, (*ValidatorSet).IsPreviousMatchingSourceAttester},
		{(*ValidatorSet).SetIsCurrentMatchingTargetAttester, (*ValidatorSet).IsCurrentMatchingTargetAttester},
		{(*ValidatorSet).SetIsPreviousMatchingTargetAttester, (*ValidatorSet).IsPreviousMatchingTargetAttester},
		{(*ValidatorSet).SetIsCurrentMatchingHeadAttester, (*ValidatorSet).IsCurrentMatchingHeadAttester},
		{(*ValidatorSet).SetIsPreviousMatchingHeadAttester, (*ValidatorSet).IsPreviousMatchingHeadAttester},
	}
	type snapshot struct {
		bits     [count][6]bool
		current  [count]*PendingAttestation
		previous [count]*PendingAttestation
	}
	randomize := func(set *ValidatorSet, want *snapshot) {
		for i := range count {
			for b, s := range bitSetters {
				v := rng.Intn(2) == 0
				s.set(set, i, v)
				want.bits[i][b] = v
			}
			if rng.Intn(3) == 0 {
				want.current[i] = &PendingAttestation{InclusionDelay: rng.Uint64()}
				set.SetMinCurrentInclusionDelayAttestation(i, want.current[i])
			}
			if rng.Intn(3) == 0 {
				want.previous[i] = &PendingAttestation{InclusionDelay: rng.Uint64()}
				set.SetMinPreviousInclusionDelayAttestation(i, want.previous[i])
			}
		}
	}
	verify := func(label string, set *ValidatorSet, want *snapshot) {
		for i := range count {
			for b, s := range bitSetters {
				require.Equal(t, want.bits[i][b], s.get(set, i), "%s: validator %d bit %d", label, i, b)
			}
			require.Same(t, want.current[i], set.MinCurrentInclusionDelayAttestation(i), "%s: validator %d current attestation", label, i)
			require.Same(t, want.previous[i], set.MinPreviousInclusionDelayAttestation(i), "%s: validator %d previous attestation", label, i)
		}
	}

	for _, progressive := range []bool{false, true} {
		src := newLiveSet(1<<20, progressive)
		for range count {
			src.appendRandom(rng)
		}
		var srcWant snapshot
		randomize(src.set, &srcWant)

		spare := newLiveSet(1<<20, progressive)
		for range count + 50 {
			spare.appendRandom(rng)
		}
		var spareWant snapshot
		randomize(spare.set, &spareWant)

		src.copyInto(spare)
		spareWant = srcWant
		verify("copy", spare.set, &spareWant)
		verify("source after copy", src.set, &srcWant)

		for i := range count {
			for b, s := range bitSetters {
				s.set(spare.set, i, !spareWant.bits[i][b])
				spareWant.bits[i][b] = !spareWant.bits[i][b]
			}
		}
		spare.set.SetMinCurrentInclusionDelayAttestation(0, nil)
		spareWant.current[0] = nil
		verify("destination after mutation", spare.set, &spareWant)
		verify("source after destination mutation", src.set, &srcWant)
		src.check(t, "source root")
		spare.check(t, "destination root")
	}
}

func forBothLayouts(t *testing.T, fn func(t *testing.T, progressive bool)) {
	for _, progressive := range []bool{false, true} {
		t.Run(fmt.Sprintf("progressive=%v", progressive), func(t *testing.T) { fn(t, progressive) })
	}
}

func newHashedLiveSet(t testing.TB, rng *rand.Rand, progressive bool, n int) *liveSet {
	t.Helper()
	live := newLiveSet(1<<20, progressive)
	for range n {
		live.appendRandom(rng)
	}
	_, err := live.hash()
	require.NoError(t, err)
	return live
}

func (l *liveSet) setRandomAt(rng *rand.Rand, i int) {
	v := randomValidatorBytes(rng)
	l.set.Set(i, v)
	l.model.set(i, v)
}

func scatteredIndexes(n int) []int {
	return []int{0, 84, 85, 255, 256, n / 2, n - 2, n - 1}
}

func TestValidatorSetIncrementalTopAfterDecode(t *testing.T) {
	const n = 3000
	forBothLayouts(t, func(t *testing.T, progressive bool) {
		rng := rand.New(rand.NewSource(23))
		source := newLiveSet(1<<20, progressive)
		for range n {
			source.appendRandom(rng)
		}
		encoded := source.model.encode()

		fresh := newLiveSet(1<<20, progressive)
		require.NoError(t, fresh.set.DecodeSSZ(encoded, 0))
		fresh.model = source.model.clone()

		reused := newHashedLiveSet(t, rng, progressive, n+700)
		require.NoError(t, reused.set.DecodeSSZ(encoded, 0))
		reused.model = source.model.clone()

		for name, live := range map[string]*liveSet{"fresh": fresh, "reused": reused} {
			live.check(t, name+" after decode")
			for round := range 3 {
				for _, i := range scatteredIndexes(n) {
					if rng.Intn(3) != 0 {
						live.setRandomAt(rng, i)
					}
				}
				live.check(t, fmt.Sprintf("%s round %d", name, round))
			}
		}
	})
}

func TestValidatorSetIncrementalTopAfterCopy(t *testing.T) {
	const n = 3000
	writes := map[string]func(rng *rand.Rand, src, dst *liveSet){
		"none": func(*rand.Rand, *liveSet, *liveSet) {},
		"same chunk": func(rng *rand.Rand, src, dst *liveSet) {
			src.setRandomAt(rng, 10)
			dst.setRandomAt(rng, 11)
		},
		"different chunks": func(rng *rand.Rand, src, dst *liveSet) {
			src.setRandomAt(rng, 10)
			dst.setRandomAt(rng, 2000)
		},
	}
	forBothLayouts(t, func(t *testing.T, progressive bool) {
		for name, write := range writes {
			for _, unhashedBeforeCopy := range []bool{false, true} {
				for _, dstWarm := range []bool{false, true} {
					for _, srcFirst := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/unhashedBeforeCopy=%v/dstWarm=%v/srcFirst=%v", name, unhashedBeforeCopy, dstWarm, srcFirst), func(t *testing.T) {
							rng := rand.New(rand.NewSource(29))
							src := newHashedLiveSet(t, rng, progressive, n)
							if unhashedBeforeCopy {
								src.setRandomAt(rng, 10)
								src.setRandomAt(rng, 1000)
							}
							dst := newLiveSet(1<<20, progressive)
							if dstWarm {
								dst = newHashedLiveSet(t, rng, progressive, n+300)
							}
							src.copyInto(dst)
							write(rng, src, dst)

							first, second := src, dst
							if !srcFirst {
								first, second = dst, src
							}
							first.check(t, "first sibling")
							second.check(t, "second sibling")

							for round := range 2 {
								write(rng, src, dst)
								src.setRandomAt(rng, n-1)
								dst.setRandomAt(rng, 255)
								first.check(t, fmt.Sprintf("first sibling round %d", round))
								second.check(t, fmt.Sprintf("second sibling round %d", round))
							}
						})
					}
				}
			}
		}
	})
}

func TestValidatorSetIncrementalTopAfterAppend(t *testing.T) {
	forBothLayouts(t, func(t *testing.T, progressive bool) {
		for _, start := range []int{255, 340, 1280, 1363, 2645, 5459} {
			t.Run(fmt.Sprint(start), func(t *testing.T) {
				rng := rand.New(rand.NewSource(31))
				live := newHashedLiveSet(t, rng, progressive, start)
				live.appendRandom(rng)
				live.check(t, "append to a clean tree")
				live.setRandomAt(rng, start-1)
				live.check(t, "write")
				for i := range 8 {
					live.appendRandom(rng)
					live.check(t, fmt.Sprintf("append %d", i))
				}
				live.setRandomAt(rng, 0)
				for range 700 {
					live.appendRandom(rng)
				}
				live.check(t, "append burst")
				live.setRandomAt(rng, start)
				live.appendRandom(rng)
				live.check(t, "write and append")
			})
		}
	})
}

func TestValidatorSetIncrementalTopAcrossLayoutSwitch(t *testing.T) {
	const n = 3000
	for _, from := range []bool{false, true} {
		t.Run(fmt.Sprintf("progressive=%v", from), func(t *testing.T) {
			rng := rand.New(rand.NewSource(37))
			live := newHashedLiveSet(t, rng, from, n)
			live.setRandomAt(rng, 1)
			live.check(t, "incremental rehash")
			for round := range 3 {
				live.progressive = !live.progressive
				live.set.SetProgressiveHashing(live.progressive)
				for _, i := range scatteredIndexes(n) {
					live.setRandomAt(rng, i)
				}
				live.check(t, fmt.Sprintf("switch %d", round))
				live.setRandomAt(rng, 2)
				live.appendRandom(rng)
				live.check(t, fmt.Sprintf("incremental after switch %d", round))
			}
		})
	}
}

func TestValidatorSetIncrementalTopAfterClear(t *testing.T) {
	forBothLayouts(t, func(t *testing.T, progressive bool) {
		rng := rand.New(rand.NewSource(41))
		live := newHashedLiveSet(t, rng, progressive, 3000)
		for _, n := range []int{0, 1, 300, 3500} {
			live.set.Clear()
			live.model = newValidatorModel(1 << 20)
			for range n {
				live.appendRandom(rng)
			}
			live.check(t, fmt.Sprintf("clear and refill to %d", n))
		}
	})
}
