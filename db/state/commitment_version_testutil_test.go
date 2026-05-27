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

package state_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func mkAddrs(firstByte byte, n int) [][]byte {
	out := make([][]byte, n)
	for i := range n {
		a := make([]byte, length.Addr)
		a[0] = firstByte
		a[1] = byte(i)
		a[length.Addr-1] = byte(i)
		out[i] = a
	}
	return out
}

func writeStepsKeys(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, keys [][]byte, fromStep, toStep uint64) {
	t.Helper()
	stepSize := agg.StepSize()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	var blockNum uint64
	for i := fromStep * stepSize; i < toStep*stepSize; i++ {
		for j := range keys {
			acc := accounts.Account{Nonce: i + 1, Balance: *uint256.NewInt(i*100_000 + uint64(j)), CodeHash: accounts.EmptyCodeHash}
			buf := accounts.SerialiseV3(&acc)
			prev, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, keys[j])
			require.NoError(t, err)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, keys[j], buf, i, prev))
		}
		if (i+1)%stepSize == 0 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, blockNum, i, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
}

func recomputeRootFromState(t *testing.T, db kv.TemporalRwDB) []byte {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	acit, err := rwTx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, -1)
	require.NoError(t, err)
	defer acit.Close()
	trieCtx := domains.GetCommitmentContext()
	for acit.HasNext() {
		k, _, err := acit.Next()
		require.NoError(t, err)
		trieCtx.TouchKey(kv.AccountsDomain, string(k), nil)
	}
	root, err := domains.ComputeCommitment(t.Context(), rwTx, false, 0, 0, "", nil)
	require.NoError(t, err)
	return root
}

// forEachCommitmentBranch iterates the branch (prefix,value) pairs of a commitment .kv file,
// skipping the commitment-state record.
func forEachCommitmentBranch(t *testing.T, path string, fn func(prefix, val []byte)) {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()
	g.Reset(0)
	var k, v []byte
	for g.HasNext() {
		k, _ = g.Next(k[:0])
		v, _ = g.Next(v[:0])
		if bytes.Equal(k, commitmentdb.KeyCommitmentState) {
			continue
		}
		fn(k, v)
	}
}

// branchKeyKindsVal counts plain vs short (referenced) keys in a single branch value.
func branchKeyKindsVal(t *testing.T, v []byte) (plain, short int) {
	t.Helper()
	_, err := commitment.BranchData(v).ReplacePlainKeys(nil, func(key []byte, isStorage bool) ([]byte, error) {
		full := length.Addr
		if isStorage {
			full = length.Addr + length.Hash
		}
		if len(key) == full {
			plain++
		} else {
			short++
		}
		return nil, nil
	})
	require.NoError(t, err)
	return plain, short
}

// branchKeyKinds counts plain vs short (referenced) keys across all branches of a commitment .kv file.
func branchKeyKinds(t *testing.T, path string) (plain, short int) {
	t.Helper()
	forEachCommitmentBranch(t, path, func(_, v []byte) {
		p, s := branchKeyKindsVal(t, v)
		plain += p
		short += s
	})
	return plain, short
}

// referencedBranchPrefixes returns the branch prefixes whose value carries short (referenced) keys.
func referencedBranchPrefixes(t *testing.T, path string) [][]byte {
	t.Helper()
	var out [][]byte
	forEachCommitmentBranch(t, path, func(k, v []byte) {
		if _, short := branchKeyKindsVal(t, v); short > 0 {
			out = append(out, bytes.Clone(k))
		}
	})
	return out
}

type commitmentKVFile struct {
	name    string
	path    string
	version version.Version
}

// commitmentKVFiles lists the commitment .kv files in dir with their parsed on-disk version.
func commitmentKVFiles(t *testing.T, dir string) []commitmentKVFile {
	t.Helper()
	ents, err := os.ReadDir(dir)
	require.NoError(t, err)
	var out []commitmentKVFile
	for _, e := range ents {
		n := e.Name()
		if !strings.Contains(n, "commitment") || !strings.HasSuffix(n, ".kv") {
			continue
		}
		ver, _, ok := strings.Cut(n, "-")
		require.True(t, ok)
		fv, err := version.ParseVersion(ver)
		require.NoError(t, err)
		out = append(out, commitmentKVFile{name: n, path: filepath.Join(dir, n), version: fv})
	}
	return out
}

// assertCommitmentVersionConsistency enforces the regime invariant: a v2.1 (plain-regime) commitment
// file must hold no short reference keys (a v2.1 file with short keys is the stale-offset corruption).
func assertCommitmentVersionConsistency(t *testing.T, dir string) (referencedFiles int) {
	t.Helper()
	for _, f := range commitmentKVFiles(t, dir) {
		_, short := branchKeyKinds(t, f.path)
		if !f.version.Less(version.V2_1) {
			require.Zerof(t, short, "v2.1+ file %s must hold only plain keys, found %d short refs (stale offsets)", f.name, short)
		} else if short > 0 {
			referencedFiles++
		}
	}
	return referencedFiles
}

// commitmentVersionCounts returns how many commitment .kv files are referenced (version < v2.1 and
// carry short keys) vs plain (version >= v2.1).
func commitmentVersionCounts(t *testing.T, dir string) (referenced, plain int) {
	t.Helper()
	for _, f := range commitmentKVFiles(t, dir) {
		if f.version.Less(version.V2_1) {
			if _, short := branchKeyKinds(t, f.path); short > 0 {
				referenced++
			}
		} else {
			plain++
		}
	}
	return referenced, plain
}

// commitmentRangeReferenced reports whether the on-disk commitment file covering the tx range is in
// the referenced regime (version < v2.1 and range >= the referencing threshold).
func commitmentRangeReferenced(t *testing.T, dir string, fileStart, fileEnd, stepSize uint64) bool {
	t.Helper()
	fromStep, toStep := fileStart/stepSize, fileEnd/stepSize
	if toStep-fromStep < 2 {
		return false
	}
	suffix := fmt.Sprintf("commitment.%d-%d.kv", fromStep, toStep)
	for _, f := range commitmentKVFiles(t, dir) {
		if strings.HasSuffix(f.name, suffix) {
			return f.version.Less(version.V2_1)
		}
	}
	return false
}

// buildMixedRegimeDatadir builds a datadir holding both v2.0-referenced and v2.1-plain commitment
// files: setA is frozen early under the flag-on (referenced) regime and never rewritten, so its
// referenced branches stay read winners; setB advances the tx range into later flag-off (plain) files.
func buildMixedRegimeDatadir(t *testing.T, stepSize, frozenSteps uint64) (kv.TemporalRwDB, *state.Aggregator, datadir.Dirs, [][]byte, [][]byte) {
	t.Helper()
	setA := mkAddrs(0x10, 12)
	setB := mkAddrs(0xf0, 12)

	db, agg := testDbAndAggregatorv3(t, stepSize)
	dirs := agg.Dirs()
	agg.SetErigondbDomainStepsInFrozenFile(frozenSteps)

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	writeStepsKeys(t, db, agg, setA, 0, frozenSteps)
	writeStepsKeys(t, db, agg, setB, frozenSteps, 2*frozenSteps)
	require.NoError(t, agg.BuildFiles(2*frozenSteps*stepSize))
	require.NoError(t, agg.MergeLoop(t.Context()))

	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	writeStepsKeys(t, db, agg, setB, 2*frozenSteps, 3*frozenSteps)
	require.NoError(t, agg.BuildFiles(3*frozenSteps*stepSize))
	require.NoError(t, agg.MergeLoop(t.Context()))

	db, agg = reopenAggregator(t, db, agg, stepSize)
	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))
	return db, agg, dirs, setA, setB
}
