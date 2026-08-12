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

	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).InMem(t, dirs.Chaindata).
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

func TestRebuildCommitmentFilesBinTargetOnHexDatadir(t *testing.T) {
	binDB, binAgg, binDirs := rebuildVariantDatadir(t)
	rebuildVariantSettingsStayHex(t, binDirs)

	binRoot, err := state.RebuildCommitmentFiles(t.Context(), binDB, &rawdbv3.TxNums, log.New(), false,
		state.WithRebuildTarget(state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie}))
	require.NoError(t, err)
	require.NotEmpty(t, binRoot)

	rebuildVariantProcessStateUntouched(t)
	rebuildVariantSettingsStayHex(t, binDirs)

	require.Equal(t, binRoot, rebuildVariantRestoredRoot(t, binDB, binAgg, commitment.VariantBinPatriciaTrie),
		"the rebuilt files must carry a bin trie state that restores to the rebuilt root")

	hexDB, hexAgg, _ := rebuildVariantDatadir(t)
	hexRoot, err := state.RebuildCommitmentFiles(t.Context(), hexDB, &rawdbv3.TxNums, log.New(), false)
	require.NoError(t, err)
	require.NotEqual(t, hexRoot, binRoot, "bin and hex commit different key spaces under different hashes")
	require.Equal(t, hexRoot, rebuildVariantRestoredRoot(t, hexDB, hexAgg, commitment.VariantHexPatriciaTrie))
}

func TestRebuildCommitmentFilesDefaultTargetIsProcessVariant(t *testing.T) {
	defaultDB, _, _ := rebuildVariantDatadir(t)
	defaultRoot, err := state.RebuildCommitmentFiles(t.Context(), defaultDB, &rawdbv3.TxNums, log.New(), false)
	require.NoError(t, err)
	require.NotEmpty(t, defaultRoot)

	hexDB, _, _ := rebuildVariantDatadir(t)
	hexRoot, err := state.RebuildCommitmentFiles(t.Context(), hexDB, &rawdbv3.TxNums, log.New(), false,
		state.WithRebuildTarget(state.RebuildTarget{Variant: commitment.VariantHexPatriciaTrie}))
	require.NoError(t, err)
	require.Equal(t, defaultRoot, hexRoot, "no target named must rebuild exactly what the hex target does")

	binDB, _, _ := rebuildVariantDatadir(t)
	binRoot, err := state.RebuildCommitmentFiles(t.Context(), binDB, &rawdbv3.TxNums, log.New(), false,
		state.WithRebuildTarget(state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie}))
	require.NoError(t, err)
	require.NotEqual(t, hexRoot, binRoot)

	// Mutates a process-wide flag, so this test never runs in parallel.
	orig := statecfg.ExperimentalBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalBinCommitment = orig })
	statecfg.ExperimentalBinCommitment = true

	pickedDB, _, _ := rebuildVariantDatadir(t)
	pickedRoot, err := state.RebuildCommitmentFiles(t.Context(), pickedDB, &rawdbv3.TxNums, log.New(), false)
	require.NoError(t, err)
	require.Equal(t, binRoot, pickedRoot, "with no target named the rebuild must follow the process variant")
}

func TestRebuildCommitmentFilesBinTargetBindsHashSuite(t *testing.T) {
	keccakDB, _, _ := rebuildVariantDatadir(t)
	keccakRoot, err := state.RebuildCommitmentFiles(t.Context(), keccakDB, &rawdbv3.TxNums, log.New(), false,
		state.WithRebuildTarget(state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: commitment.PBinHashKeccak}))
	require.NoError(t, err)
	require.Equal(t, commitment.PBinHashKeccak, commitment.PBinHashSuiteName())

	blake3DB, _, _ := rebuildVariantDatadir(t)
	blake3Root, err := state.RebuildCommitmentFiles(t.Context(), blake3DB, &rawdbv3.TxNums, log.New(), false,
		state.WithRebuildTarget(state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: commitment.PBinHashBlake3}))
	require.NoError(t, err)
	require.NotEqual(t, keccakRoot, blake3Root, "H is part of the tree, so the two suites cannot agree")
	require.Equal(t, commitment.PBinHashKeccak, commitment.PBinHashSuiteName(),
		"the rebuild must restore the suite it bound for its own run")
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
}
