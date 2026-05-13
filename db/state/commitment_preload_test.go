// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package state

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// TestCommitmentPreloadPhase1Cursor mirrors triggerTrunkPreload's phase-1
// dbPrefetch in db/state/execctx/domain_shared.go: a DupSort cursor over
// TblCommitmentVals walked via Seek + NextNoDup, decoding v[8:] as the value
// (dups = invertedStep||value, latest-first). Asserts that for a populated
// CommitmentDomain DB, the walk over the contract's two parity ranges
// (commitment.ContractTrunkKeyRanges) picks up the LATEST value per key and
// excludes keys outside the ranges (foreign contract).
//
// The preload-side unit test (commitment.TestPreloadParallel_DbHitsShadowFiles)
// covers the algorithm with a fake dbBranches map; this covers the cursor
// mechanics against the real domain writer encoding.
func TestCommitmentPreloadPhase1Cursor(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	logger := log.New()
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.CommitmentDomain, 16, logger)

	ctx := t.Context()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domainRoTx := d.beginForTests()
	defer domainRoTx.Close()
	writer := domainRoTx.NewWriter()
	defer writer.Close()

	// Pick two contracts so we can also assert foreign-contract keys are
	// excluded from the target contract's parity ranges.
	contractA := make([]byte, 32)
	for i := range contractA {
		contractA[i] = byte(0x10 + i)
	}
	contractB := make([]byte, 32)
	for i := range contractB {
		contractB[i] = byte(0x80 - i)
	}
	nibA := commitment.ContractNibbles(contractA)
	nibB := commitment.ContractNibbles(contractB)

	// keyAt builds a real commitment-domain key: HexToCompact(contractNibbles
	// ++ slotPath). The path's parity falls out of len(slotPath).
	keyAt := func(contractNibbles, slotPath []byte) []byte {
		full := append(append([]byte{}, contractNibbles...), slotPath...)
		k := nibbles.HexToCompact(full)
		return common.Copy(k)
	}

	// A small set spanning even (depths 64, 66, 68) and odd (65, 67, 69) parity
	// for contractA, plus a foreign key under contractB that must NOT show up.
	slotPaths := [][]byte{
		{},                        // depth 64 (even, == subtree root)
		{0x3},                     // depth 65 (odd)
		{0x4, 0x5},                // depth 66 (even)
		{0x6, 0x7, 0x8},           // depth 67 (odd)
		{0x9, 0xa, 0xb, 0xc},      // depth 68 (even)
		{0xd, 0xe, 0xf, 0x0, 0x1}, // depth 69 (odd)
	}

	// Write each key at multiple txNums so it has multiple steps in MDBX (the
	// DupSort dups). prev tracks the previous value (PutWithPrev contract).
	expected := map[string][]byte{}
	prev := map[string][]byte{}
	writeAt := func(key []byte, txNum uint64, marker byte) {
		v := make([]byte, 8)
		v[0] = marker
		v[7] = byte(txNum)
		err := writer.PutWithPrev(key, v, txNum, prev[string(key)])
		require.NoError(t, err)
		prev[string(key)] = common.Copy(v)
		expected[string(key)] = common.Copy(v)
	}

	// 3 successive writes per key at txNums 1, 5, 9 (latest = txNum 9 — the
	// value we expect the cursor to return).
	for i, sp := range slotPaths {
		k := keyAt(nibA, sp)
		for ti, tn := range []uint64{1, 5, 9} {
			writeAt(k, tn, byte(0xA0+i*4+ti)) // distinctive marker per (key, step)
		}
	}
	// Foreign contract key — must NOT appear in contractA's ranges.
	foreign := keyAt(nibB, []byte{0x4, 0x5})
	writeAt(foreign, 7, 0xFF)

	require.NoError(t, writer.Flush(ctx, tx))

	// Walk the cursor exactly like dbPrefetch does.
	evenFrom, evenTo, oddFrom, oddTo := commitment.ContractTrunkKeyRanges(nibA)
	c, err := tx.CursorDupSort(kv.TblCommitmentVals)
	require.NoError(t, err)
	defer c.Close()

	got := map[string][]byte{}
	scanRange := func(from, to []byte) {
		k, v, err := c.Seek(from)
		for ; k != nil; k, v, err = c.NextNoDup() {
			require.NoError(t, err)
			if bytes.Compare(k, to) >= 0 {
				break
			}
			require.GreaterOrEqual(t, len(v), 8, "dup value must be invStep(8B)||value")
			got[string(common.Copy(k))] = common.Copy(v[8:])
		}
	}
	scanRange(evenFrom, evenTo)
	scanRange(oddFrom, oddTo)

	// Every contractA branch must be present with its LATEST value (txNum=9).
	for _, sp := range slotPaths {
		k := keyAt(nibA, sp)
		want, ok := expected[string(k)]
		require.True(t, ok)
		g, present := got[string(k)]
		require.Truef(t, present, "key %x (depth %d) missing from cursor walk", k, 64+len(sp))
		require.Equalf(t, want, g, "key %x: cursor returned the wrong value (not the latest step)", k)
	}
	// Foreign-contract key must be absent.
	_, leaked := got[string(foreign)]
	require.Falsef(t, leaked, "foreign key %x leaked into contractA's parity ranges", foreign)
}
