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

package commitment

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// pbinDedupSpy counts the code reads the chunking makes, the per-account cost
// the shared-hash cache is there to drop.
type pbinDedupSpy struct {
	*MockState
	reads int
}

func (s *pbinDedupSpy) Code(plainKey []byte) ([]byte, error) {
	s.reads++
	return s.MockState.Code(plainKey)
}

type pbinDedupRun struct {
	keys      []string
	chunkKeys []string
	queued    int // chunks the flush had to sort
	reads     int
	stats     PBinCodeStats
}

func (r pbinDedupRun) count(key []byte) int {
	n := 0
	for _, k := range r.keys {
		if k == hex.EncodeToString(key) {
			n++
		}
	}
	return n
}

// pbinDedupStream runs a corpus through one update stream and reports what the
// chunking cost. Reusing the stream across calls is what a pooled engine does,
// so a cache that outlives a Process shows up here.
func pbinDedupStream(t *testing.T, s *pbinUpdateStream, spy *pbinDedupSpy, c *pbinTestCorpus) pbinDedupRun {
	t.Helper()

	before := spy.reads
	run := pbinDedupRun{queued: -1}
	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), c.plainKeys, c.updates)
	_, err := s.process(context.Background(), upd, spy, func(treeKey, _ []byte, _ *Update) error {
		if treeKey[0] == pbinCodeZone {
			if run.queued < 0 {
				run.queued = len(s.codeChunks)
			}
			run.chunkKeys = append(run.chunkKeys, hex.EncodeToString(treeKey))
		}
		run.keys = append(run.keys, hex.EncodeToString(treeKey))
		return nil
	})
	require.NoError(t, err)
	run.reads = spy.reads - before
	run.stats = s.codeStats
	return run
}

func pbinDedupSpyFor(t *testing.T, c *pbinTestCorpus) *pbinDedupSpy {
	t.Helper()
	ms := NewMockState(t)
	c.applyTo(t, ms)
	return &pbinDedupSpy{MockState: ms}
}

// pbinSharedCodeCorpus is n accounts holding byte-identical code.
func pbinSharedCodeCorpus(n int, code []byte) *pbinTestCorpus {
	c := new(pbinTestCorpus)
	for i := range n {
		c.accountWithCodeBytes(pbinOracleAddr(uint64(90+i)), uint64(i+1), 100, code)
	}
	return c
}

// TestPBinSharedCodeIsChunkedOncePerHash: chunk keys are content-addressed, so
// accounts sharing code share their chunk leaves. Chunking each of them writes
// the same leaves again and leaves the flush to sort them out.
func TestPBinSharedCodeIsChunkedOncePerHash(t *testing.T) {
	t.Parallel()

	const accounts = 4
	code := pbinTestCode(200)
	chunks := len(pbinChunkifyCode(code))
	require.Equal(t, 7, chunks)

	corpus := pbinSharedCodeCorpus(accounts, code)
	spy := pbinDedupSpyFor(t, corpus)
	run := pbinDedupStream(t, new(pbinUpdateStream), spy, corpus)

	require.Equal(t, 1, run.reads, "the code domain is read once per code hash")
	require.Equal(t, PBinCodeStats{CodeBearingAccounts: accounts, UniqueCodeHashes: 1}, run.stats)
	require.Equal(t, chunks, run.queued, "the flush sorts one chunk set, not one per account")
	require.Len(t, run.chunkKeys, chunks)

	// The header leaves stay per account: only the chunks are shared.
	for i := range accounts {
		addr := pbinOracleAddr(uint64(90 + i))
		require.Equal(t, 1, run.count(pbinTreeKeyAccount(addr, pbinBasicDataLeafKey)))
		require.Equal(t, 1, run.count(pbinTreeKeyAccount(addr, pbinCodeHashLeafKey)))
		require.Equal(t, 1, run.count(pbinTreeKeyAccount(addr, pbinDelegationLeafKey)))
	}

	_, root := corpus.process(t)
	require.Equal(t, corpus.oracleRoot(t), root, "sharing the chunking must not move the root")
}

// The cache is cleared per Process: an unwind between two of them rolls the tree
// back, and a chunk skipped against a stale hit would never be re-inserted.
func TestPBinChunkCacheDoesNotOutliveAProcess(t *testing.T) {
	t.Parallel()

	code := pbinTestCode(200)
	chunks := len(pbinChunkifyCode(code))
	corpus := pbinSharedCodeCorpus(2, code)
	spy := pbinDedupSpyFor(t, corpus)

	stream := new(pbinUpdateStream)
	first := pbinDedupStream(t, stream, spy, corpus)
	second := pbinDedupStream(t, stream, spy, corpus)

	require.Equal(t, first.queued, second.queued)
	require.Equal(t, chunks, second.queued, "the second Process re-chunks the code it no longer knows")
	require.Equal(t, 1, second.reads)

	require.Equal(t, first.stats, second.stats)
	require.Equal(t, PBinCodeStats{CodeBearingAccounts: 2, UniqueCodeHashes: 1}, second.stats,
		"the counts describe one Process, so they are cleared with the cache")

	stream.reset()
	require.Empty(t, stream.codeSeen, "reset clears the cache; Release alone would keep it across a pooled reuse")
	require.Zero(t, stream.codeStats)
}

// A hash naming two code sizes means one of the two reads is wrong, and the
// chunks the second account skips would be the other account's.
func TestPBinSharedCodeSizeConflictIsRefused(t *testing.T) {
	t.Parallel()

	code := pbinTestCode(62)
	longer := pbinTestCode(93)
	codeHash := keccak.Sum256(code)

	honest, liar := pbinOracleAddr(96), pbinOracleAddr(97)
	corpus := new(pbinTestCorpus).
		accountWithCode(honest, 1, 10, codeHash, uint64(len(code))).
		accountWithCode(liar, 2, 20, codeHash, uint64(len(longer)))

	ms := NewMockState(t)
	corpus.applyTo(t, ms)
	ms.setCode(honest, code)
	ms.setCode(liar, longer)

	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), corpus.plainKeys, corpus.updates)
	_, err := new(pbinUpdateStream).process(context.Background(), upd, ms, func([]byte, []byte, *Update) error { return nil })
	require.ErrorIs(t, err, errPBinCodeSizeConflict)
}

// The size gate runs before the cache is consulted, so a sizeless account is
// refused however many accounts with that code came before it.
func TestPBinCodeSizeGateRunsBeforeTheChunkCache(t *testing.T) {
	t.Parallel()

	code := pbinTestCode(62)
	codeHash := keccak.Sum256(code)
	sized, sizeless := pbinOracleAddr(98), pbinOracleAddr(99)

	corpus := new(pbinTestCorpus).
		accountWithCode(sized, 1, 10, codeHash, uint64(len(code))).
		accountWithCode(sizeless, 2, 20, codeHash, 0)
	ms := NewMockState(t)
	corpus.applyTo(t, ms)
	ms.setCode(sized, code)
	ms.setCode(sizeless, code)

	s := &pbinUpdateStream{state: ms}
	_, _, err := s.chunkSource(sized, &corpus.updates[0])
	require.NoError(t, err)
	require.Contains(t, s.codeSeen, common.Hash(codeHash))

	_, _, err = s.chunkSource(sizeless, &corpus.updates[1])
	require.ErrorIs(t, err, errPBinCodeSizeMissing)
}

// A delegation indicator is 0xef0100 followed by the target, so accounts
// delegating to one implementation share a code hash. Caching them would take
// the second account down the code path, which writes the wrong header leaves.
func TestPBinDelegationsAreNeverCached(t *testing.T) {
	t.Parallel()

	indicator := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0x5A}, 20)...)
	first, second := pbinOracleAddr(101), pbinOracleAddr(102)
	corpus := new(pbinTestCorpus).
		accountWithCodeBytes(first, 1, 10, indicator).
		accountWithCodeBytes(second, 2, 20, indicator)

	spy := pbinDedupSpyFor(t, corpus)
	run := pbinDedupStream(t, new(pbinUpdateStream), spy, corpus)

	require.Equal(t, 2, run.reads, "the indicator has to be read per account: it decides the header leaves")
	require.Empty(t, run.chunkKeys, "a delegation indicator is never chunked")
	for _, addr := range [][]byte{first, second} {
		require.Equal(t, 1, run.count(pbinTreeKeyAccount(addr, pbinDelegationLeafKey)))
		require.Equal(t, 1, run.count(pbinTreeKeyAccount(addr, pbinCodeHashLeafKey)))
	}

	_, root := corpus.process(t)
	require.Equal(t, corpus.oracleRoot(t), root)
}

// A witness pass keys its chunks on the code the block writes, not on the
// account's state code hash, so a cache keyed on the hash would name other
// chunks. It keeps out of the cache entirely and chunks per account.
func TestPBinWitnessPassChunksEveryAccount(t *testing.T) {
	t.Parallel()

	code := pbinTestCode(200)
	created := pbinTestCode(124)
	corpus := pbinSharedCodeCorpus(2, code)
	overridden := pbinOracleAddr(91)

	ms := NewMockState(t)
	corpus.applyTo(t, ms)

	block := PBinWitnessBlock{Code: map[string][]byte{string(overridden): created}}
	s := &pbinUpdateStream{witness: block, witnessPass: true}
	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), corpus.plainKeys, corpus.updates)
	proved := make(map[string]struct{})
	_, err := s.process(context.Background(), upd, ms, func(treeKey, _ []byte, _ *Update) error {
		proved[hex.EncodeToString(treeKey)] = struct{}{}
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, s.codeSeen, "a witness pass must leave the cache alone")

	// The proved keys are the fold's, plus the chunks of the code the block
	// writes: whatever the pass skips, a verifier cannot reach.
	want := make(map[string]struct{})
	for _, k := range pbinStreamKeys(t, ms, corpus, PBinWitnessBlock{}, false) {
		want[k] = struct{}{}
	}
	for i := range pbinChunkifyCode(created) {
		want[hex.EncodeToString(pbinTreeKeyCodeChunk(keccak.Sum256(created), i))] = struct{}{}
	}
	require.Equal(t, want, proved)

	for i := range pbinChunkifyCode(code) {
		require.Contains(t, proved, hex.EncodeToString(pbinTreeKeyCodeChunk(keccak.Sum256(code), i)),
			"the state code's chunk %d is not walked", i)
	}
}
