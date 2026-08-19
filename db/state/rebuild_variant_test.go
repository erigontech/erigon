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

// A commitment rebuild is a producer of commitment files, so the scheme it
// produces is a parameter of the run: asking for bin output while pointed at a
// hex datadir is the offline migration case, not a misconfiguration.
package state_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const (
	rebuildVariantStepSize = uint64(8)
	rebuildVariantSteps    = 4
	rebuildVariantAccounts = 6
	rebuildVariantSlots    = 4
)

func rebuildVariantAddr(i int) []byte {
	a := make([]byte, length.Addr)
	a[0] = 0xb0
	a[1] = byte(i)
	a[length.Addr-1] = byte(i*11 + 5)
	return a
}

func rebuildVariantSlotKey(addr []byte, j int) []byte {
	k := make([]byte, length.Addr+length.Hash)
	copy(k, addr)
	k[length.Addr] = byte(j)
	k[len(k)-1] = byte(j*17 + 2)
	return k
}

func rebuildVariantTrieCfg(v commitment.TrieVariant) commitment.TrieConfig {
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = v
	cfg.EnableTrieWarmup = false
	return cfg
}

func rebuildVariantAgg(t *testing.T, rawDB kv.RwDB, dirs datadir.Dirs) *state.Aggregator {
	t.Helper()
	agg := state.NewTest(dirs).StepSize(rebuildVariantStepSize).Logger(log.New()).MustOpen(t.Context(), rawDB)
	t.Cleanup(agg.Close)
	// The bin trie refuses referenced branches; production resolves that from
	// erigondb.toml, NewTest bypasses it.
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	require.NoError(t, agg.OpenFolder())
	return agg
}

// A hex-configured datadir holding account and storage files and no commitment
// at all: the input an offline rebuild is pointed at. Deterministic, so two
// calls produce datadirs a rebuild must derive the same root from.
func rebuildVariantDatadir(t *testing.T) (kv.TemporalRwDB, *state.Aggregator, datadir.Dirs) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE),
		fmt.Appendf(nil, "step_size = %d\nsteps_in_frozen_file = 8\nreferences_in_commitment_branches = false\n", rebuildVariantStepSize), 0644))

	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).InMem(dirs.Chaindata).
		GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(rawDB.Close)

	agg := rebuildVariantAgg(t, rawDB, dirs)
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)

	txCount := rebuildVariantSteps * rebuildVariantStepSize
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New(),
		execctx.WithTrieConfig(rebuildVariantTrieCfg(commitment.VariantHexPatriciaTrie)))
	require.NoError(t, err)
	defer sd.Close()
	sd.DiscardWrites(kv.CommitmentDomain)

	for txNum := range txCount {
		for i := range rebuildVariantAccounts {
			addr := rebuildVariantAddr(i)
			acc := accounts.Account{
				Nonce:    txNum + 1,
				Balance:  *uint256.NewInt(txNum*1_000 + uint64(i)),
				CodeHash: accounts.EmptyCodeHash,
			}
			prev, _, err := sd.GetLatest(kv.AccountsDomain, rwTx, addr)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), txNum, prev))

			for j := range rebuildVariantSlots {
				sk := rebuildVariantSlotKey(addr, j)
				val := []byte{byte(txNum + 1), byte(i + 1), byte(j + 1)}
				prev, _, err := sd.GetLatest(kv.StorageDomain, rwTx, sk)
				require.NoError(t, err)
				require.NoError(t, sd.DomainPut(kv.StorageDomain, rwTx, sk, val, txNum, prev))
			}
		}
	}
	require.NoError(t, sd.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(txCount))

	// Collation seals a commitment file per step even with the writes discarded,
	// and a rebuild takes any file covering a range as that range already done.
	agg.Close()
	paths, err := dir.ListFiles(dirs.SnapDomain)
	require.NoError(t, err)
	for _, p := range paths {
		if strings.Contains(filepath.Base(p), kv.CommitmentDomain.String()) {
			require.NoError(t, dir.RemoveFile(p))
		}
	}
	agg = rebuildVariantAgg(t, rawDB, dirs)
	db, err = temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	return db, agg, dirs
}

// The root a fresh SharedDomains restores from the rebuilt commitment state,
// folding nothing: only files written by the named engine restore under it.
func rebuildVariantRestoredRoot(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, v commitment.TrieVariant) []byte {
	t.Helper()
	require.NoError(t, agg.OpenFolder())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), 1))

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(rebuildVariantTrieCfg(v)))
	require.NoError(t, err)
	defer sd.Close()
	root, err := sd.GetCommitmentCtx().Trie().RootHash()
	require.NoError(t, err)
	return root
}

func rebuildVariantSettingsStayHex(t *testing.T, dirs datadir.Dirs) {
	t.Helper()
	settings, err := state.ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err, "a hex datadir must still resolve cleanly after a bin rebuild")
	require.Equal(t, state.TrieVariantHex, settings.TrieVariantName())
}

func rebuildVariantProcessStateUntouched(t *testing.T) {
	t.Helper()
	require.False(t, statecfg.ExperimentalBinCommitment, "the rebuild must not enable the bin flag process-wide")
	require.Empty(t, statecfg.BinCommitmentHash)
	require.Equal(t, commitment.VariantHexPatriciaTrie, execctx.PickTrieVariant())
	require.Equal(t, commitment.PBinHashKeccak, commitment.PBinHashSuiteName(), "the rebuild must restore H it bound")
}

// The counts a caller can only get by return: the rebuild logs them too, but a
// report scraped from logs is not a report.
func rebuildVariantReportCounts(t *testing.T, report *state.RebuildReport, root []byte, v commitment.TrieVariant) {
	t.Helper()
	require.NotNil(t, report)
	require.Equal(t, v, report.Target.Variant)
	require.NotEmpty(t, report.Ranges)

	for _, r := range report.Ranges {
		require.NotEmpty(t, r.Shards, "a rebuilt range walks at least one shard")
		require.Positive(t, r.KeysInFiles)
		require.Positive(t, r.KeysProcessed)
		require.Less(t, r.StepFrom, r.StepTo)

		var shardKeys uint64
		for _, s := range r.Shards {
			shardKeys += s.Keys
			require.LessOrEqual(t, s.UniqueCodeHashes, s.CodeBearingAccounts,
				"a code hash cannot be chunked more often than the accounts holding it")
		}
		require.Equal(t, r.KeysProcessed, shardKeys, "the shards must account for every key the range walked")
	}
	require.Equal(t, root, report.Ranges[len(report.Ranges)-1].RootHash)
}

func TestRebuildCommitmentFilesBinTargetOnHexDatadir(t *testing.T) {
	binDB, binAgg, binDirs := rebuildVariantDatadir(t)
	rebuildVariantSettingsStayHex(t, binDirs)

	binRoot, binReport, err := state.RebuildCommitmentFiles(t.Context(), binDB, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie})
	require.NoError(t, err)
	require.NotEmpty(t, binRoot)
	rebuildVariantReportCounts(t, binReport, binRoot, commitment.VariantBinPatriciaTrie)

	rebuildVariantProcessStateUntouched(t)
	rebuildVariantSettingsStayHex(t, binDirs)

	require.Equal(t, binRoot, rebuildVariantRestoredRoot(t, binDB, binAgg, commitment.VariantBinPatriciaTrie),
		"the rebuilt files must carry a bin trie state that restores to the rebuilt root")

	hexDB, hexAgg, _ := rebuildVariantDatadir(t)
	hexRoot, hexReport, err := state.RebuildCommitmentFiles(t.Context(), hexDB, &rawdbv3.TxNums, log.New(), false, state.RebuildTarget{})
	require.NoError(t, err)
	rebuildVariantReportCounts(t, hexReport, hexRoot, commitment.VariantHexPatriciaTrie)
	require.NotEqual(t, hexRoot, binRoot, "bin and hex commit different key spaces under different hashes")
	require.Equal(t, hexRoot, rebuildVariantRestoredRoot(t, hexDB, hexAgg, commitment.VariantHexPatriciaTrie))
}

// The commitment files a rebuild left behind, by name and content: a resumed run
// must neither rewrite nor add to them.
func rebuildVariantCommitmentFiles(t *testing.T, dirs datadir.Dirs) map[string]string {
	t.Helper()
	paths, err := dir.ListFiles(dirs.SnapDomain)
	require.NoError(t, err)
	got := map[string]string{}
	for _, p := range paths {
		name := filepath.Base(p)
		if !strings.Contains(name, kv.CommitmentDomain.String()) {
			continue
		}
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		got[name] = string(data)
	}
	return got
}

// A resume takes any commitment file covering a range as that range done. The
// skip reads the files, not the scheme that wrote them, so a bin target resumes
// over its own output like a hex one does.
func TestRebuildCommitmentFilesBinTargetResumeSkipsCoveredRanges(t *testing.T) {
	db, _, dirs := rebuildVariantDatadir(t)
	target := state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie}

	_, report, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false, target)
	require.NoError(t, err)
	require.NotEmpty(t, report.Ranges)

	built := rebuildVariantCommitmentFiles(t, dirs)
	require.NotEmpty(t, built)

	_, resumed, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false, target)
	require.NoError(t, err)
	require.Empty(t, resumed.Ranges, "every range is covered, so the resumed run walks none of them")
	require.Equal(t, built, rebuildVariantCommitmentFiles(t, dirs))
}

func TestRebuildCommitmentFilesDefaultTargetIsProcessVariant(t *testing.T) {
	defaultDB, _, _ := rebuildVariantDatadir(t)
	defaultRoot, _, err := state.RebuildCommitmentFiles(t.Context(), defaultDB, &rawdbv3.TxNums, log.New(), false, state.RebuildTarget{})
	require.NoError(t, err)
	require.NotEmpty(t, defaultRoot)

	hexDB, _, _ := rebuildVariantDatadir(t)
	hexRoot, _, err := state.RebuildCommitmentFiles(t.Context(), hexDB, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{Variant: commitment.VariantHexPatriciaTrie})
	require.NoError(t, err)
	require.Equal(t, defaultRoot, hexRoot, "no target named must rebuild exactly what the hex target does")

	binDB, _, _ := rebuildVariantDatadir(t)
	binRoot, _, err := state.RebuildCommitmentFiles(t.Context(), binDB, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie})
	require.NoError(t, err)
	require.NotEqual(t, hexRoot, binRoot)

	// Mutates a process-wide flag, so this test never runs in parallel.
	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = true

	pickedDB, _, _ := rebuildVariantDatadir(t)
	pickedRoot, _, err := state.RebuildCommitmentFiles(t.Context(), pickedDB, &rawdbv3.TxNums, log.New(), false, state.RebuildTarget{})
	require.NoError(t, err)
	require.Equal(t, binRoot, pickedRoot, "with no target named the rebuild must follow the process variant")
}

func TestRebuildCommitmentFilesBinTargetBindsHashSuite(t *testing.T) {
	keccakDB, _, _ := rebuildVariantDatadir(t)
	keccakRoot, _, err := state.RebuildCommitmentFiles(t.Context(), keccakDB, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: commitment.PBinHashKeccak})
	require.NoError(t, err)
	require.Equal(t, commitment.PBinHashKeccak, commitment.PBinHashSuiteName())

	blake3DB, _, _ := rebuildVariantDatadir(t)
	blake3Root, _, err := state.RebuildCommitmentFiles(t.Context(), blake3DB, &rawdbv3.TxNums, log.New(), false,
		state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: commitment.PBinHashBlake3})
	require.NoError(t, err)
	require.NotEqual(t, keccakRoot, blake3Root, "H is part of the tree, so the two suites cannot agree")
	require.Equal(t, commitment.PBinHashKeccak, commitment.PBinHashSuiteName(),
		"the rebuild must restore the suite it bound for its own run")
}

// The hash flag and the selected suite are two different things: a tool that binds
// its own suite per run (the integration rebuild) never calls SetPBinHashSuite, so
// reading the suite back would silently answer keccak for --...hash=blake3.
func TestDefaultRebuildTargetFollowsTheConfiguredHash(t *testing.T) {
	// Mutates process-wide flags, so this test never runs in parallel.
	bin, hash := statecfg.ExperimentalBinCommitment, statecfg.BinCommitmentHash
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment, statecfg.BinCommitmentHash = bin, hash })

	statecfg.ExperimentalBinCommitment = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.Equal(t, commitment.PBinHashBlake3, state.DefaultRebuildTarget().HashName)

	statecfg.BinCommitmentHash = ""
	require.Equal(t, commitment.PBinHashSuiteName(), state.DefaultRebuildTarget().HashName,
		"with no flag the target keeps the suite this process selected")
}

func TestRebuildTargetResolve(t *testing.T) {
	hex, err := state.RebuildTarget{Variant: commitment.VariantHexPatriciaTrie}.Resolve()
	require.NoError(t, err)
	require.Equal(t, commitment.VariantHexPatriciaTrie, hex.Variant)
	require.Empty(t, hex.HashName, "H is meaningless outside the bin trie")

	bin, err := state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie}.Resolve()
	require.NoError(t, err)
	require.Equal(t, commitment.PBinHashKeccak, bin.HashName)

	_, err = state.RebuildTarget{Variant: commitment.VariantHexPatriciaTrie, HashName: commitment.PBinHashBlake3}.Resolve()
	require.Error(t, err)

	_, err = state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: "sha3"}.Resolve()
	require.Error(t, err)

	_, err = state.RebuildTarget{Variant: "verkle"}.Resolve()
	require.Error(t, err)

	unset, err := state.RebuildTarget{}.Resolve()
	require.NoError(t, err)
	require.Equal(t, state.DefaultRebuildTarget(), unset)

	require.Zero(t, unset.MaxShardSteps, "an unset shard size stays unset, to be derived per range")
	require.Zero(t, hex.MaxShardSteps)
	require.Zero(t, bin.MaxShardSteps)

	pinned, err := state.RebuildTarget{MaxShardSteps: 16}.Resolve()
	require.NoError(t, err)
	require.Equal(t, uint64(16), pinned.MaxShardSteps, "an explicit shard size survives resolution")
}
