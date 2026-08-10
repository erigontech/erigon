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
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// Code reclamation on account removal. A removed account's chunk leaves are
// dropped only if they were absent from the parent state and no account in the
// batch's post-state holds the code hash — and both together mean the leaves
// were never inserted, since an account created and destroyed inside one batch
// merges to a bare deletion before the stream sees it. So the engine keeps
// every chunk leaf it holds, and these tests pin the three directions of that
// rule.

// pbinMergedRemoval is the update an in-batch create-and-destroy merges to: a
// bare deletion still carrying the code fields the create touched. The stream
// must treat it as codeless — the account's code is gone from the code domain.
func pbinMergedRemoval(code []byte) Update {
	return Update{Flags: DeleteUpdate, CodeHash: keccak.Sum256(code), CodeSize: uint64(len(code))}
}

func pbinTestProcessMerged(t *testing.T, pph *PBinPatriciaHashed, plainKeys [][]byte, updates []Update) []byte {
	t.Helper()
	upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), plainKeys, updates)
	root, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return root
}

func pbinTestChunkEntries(entries []pbinOracleEntry, code []byte) []pbinOracleEntry {
	codeHash := keccak.Sum256(code)
	for i, chunk := range pbinChunkifyCode(code) {
		entries = append(entries, pbinOracleEntry{key: pbinTreeKeyCodeChunk(codeHash, i), value: bytes.Clone(chunk[:])})
	}
	return entries
}

func TestPBinReclaimDropsCodeWithNoSurvivor(t *testing.T) {
	t.Parallel()

	bystander := pbinOracleAddr(91)
	code := bytes.Repeat([]byte{0x5B}, 31*3)
	stored := new(pbinTestCorpus).account(bystander, 1, 2, common.Hash{0x91})

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(stored.plainKeys, stored.updates))

	plainKeys := append([][]byte{pbinOracleAddr(92)}, stored.plainKeys...)
	updates := append([]Update{pbinMergedRemoval(code)}, stored.updates...)
	root := pbinTestProcessMerged(t, pph, plainKeys, updates)

	require.Equal(t, stored.oracleRoot(t), root,
		"a code deployed and destroyed inside the batch leaves no chunk leaf")

	withChunks := pbinOracleRoot(pbinTestChunkEntries(stored.entries(t), code))
	require.NotEqual(t, withChunks[:], root, "keeping the dead code's chunks must change the root")
}

func TestPBinReclaimKeepsCodeForBatchSibling(t *testing.T) {
	t.Parallel()

	sibling := pbinOracleAddr(93)
	code := bytes.Repeat([]byte{0x5B}, 31*3)
	survivors := new(pbinTestCorpus).accountWithCodeBytes(sibling, 1, 5, code)

	pph, ms := pbinTestEngine(t)
	survivors.applyTo(t, ms)

	plainKeys := append([][]byte{pbinOracleAddr(94)}, survivors.plainKeys...)
	updates := append([]Update{pbinMergedRemoval(code)}, survivors.updates...)
	root := pbinTestProcessMerged(t, pph, plainKeys, updates)

	require.Equal(t, survivors.oracleRoot(t), root,
		"a sibling written in the same batch keeps the shared chunk set")

	noChunks := new(pbinTestCorpus).accountWithCode(sibling, 1, 5, keccak.Sum256(code), uint64(len(code)))
	require.NotEqual(t, noChunks.oracleRoot(t), root, "the surviving holder's chunks must stay in the tree")
}

// TestPBinReclaimKeepsCodeForPreexistingHolder is the case a referenced-set
// rule gets wrong: the code hash never appears in the removal batch except on
// the deletion itself, and the untouched holder's leaves must survive.
func TestPBinReclaimKeepsCodeForPreexistingHolder(t *testing.T) {
	t.Parallel()

	holder := pbinOracleAddr(95)
	code := bytes.Repeat([]byte{0x5B}, 31*3)
	stored := new(pbinTestCorpus).accountWithCodeBytes(holder, 2, 9, code)

	pph, ms := pbinTestEngine(t)
	stored.applyTo(t, ms)
	before := pbinTestProcess(t, pph, stored.plainKeys, stored.updates)

	pph.Reset()
	root := pbinTestProcessMerged(t, pph, [][]byte{pbinOracleAddr(96)}, []Update{pbinMergedRemoval(code)})

	require.Equal(t, before, root, "an untouched pre-existing holder keeps its code")
	require.Equal(t, stored.oracleRoot(t), root)

	noChunks := new(pbinTestCorpus).accountWithCode(holder, 2, 9, keccak.Sum256(code), uint64(len(code)))
	require.NotEqual(t, noChunks.oracleRoot(t), root, "dropping the holder's chunks must change the root")
}
