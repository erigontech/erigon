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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type pbinRebuildCodeShape uint8

const (
	pbinRebuildDelegation pbinRebuildCodeShape = iota
	pbinRebuildContract
)

const pbinRebuildCodeAccounts = 14

func pbinRebuildCodeAddress(shape pbinRebuildCodeShape, index int) []byte {
	address := make([]byte, length.Addr)
	address[0] = 0xd0 + byte(shape)
	address[1] = byte(index)
	address[length.Addr-1] = byte(index*13 + 3)
	return address
}

func pbinRebuildCode(shape pbinRebuildCodeShape, address []byte) []byte {
	if shape == pbinRebuildDelegation {
		return append(append([]byte(nil), eip8297.DelegationMarker[:]...), address...)
	}
	return []byte{0x60, 0x00, 0x56, byte(shape)}
}

func pbinRebuildCodeDatadir(t *testing.T, shape pbinRebuildCodeShape) (kv.TemporalRwDB, []eip8297.State) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	stepSize := shardTombstoneStepSize
	frozenSteps := shardTombstoneFrozenSteps
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE),
		fmt.Appendf(nil, "step_size = %d\nsteps_in_frozen_file = %d\nreferences_in_commitment_branches = false\n", stepSize, frozenSteps), 0o644))
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).InMem(dirs.Chaindata).
		GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(rawDB.Close)
	agg := shardTombstoneAgg(t, rawDB, dirs)
	tdb, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	t.Cleanup(tdb.Close)
	var db kv.TemporalRwDB = tdb

	rangeTxCount := uint64(shardTombstoneRange1Steps * shardTombstoneStepSize)
	pbinWriteCodeRange(t, db, 0, rangeTxCount, shape, false)
	require.NoError(t, agg.BuildFiles(db, rangeTxCount, unboundedFinalityCtx))
	agg, db = reopenShardTombstoneAgg(t, agg, rawDB, dirs)
	pbinWriteCodeRange(t, db, rangeTxCount, rangeTxCount, shape, true)
	require.NoError(t, agg.BuildFiles(db, rangeTxCount*2, unboundedFinalityCtx))
	agg, db = reopenShardTombstoneAgg(t, agg, rawDB, dirs)
	pbinWriteCodeGuard(t, db, rangeTxCount*2, shape)
	require.NoError(t, agg.BuildFiles(db, rangeTxCount*2+shardTombstoneStepSize, unboundedFinalityCtx))
	db = trimShardTombstoneCommitment(t, agg, rawDB, dirs)

	states := make([]eip8297.State, 0, pbinRebuildCodeAccounts)
	for i := range pbinRebuildCodeAccounts {
		address := pbinRebuildCodeAddress(shape, i)
		states = append(states, eip8297.State{
			Address: address,
			Nonce:   2,
			Balance: *uint256.NewInt(uint64(i) + 17),
			Slots:   make(map[string][]byte),
		})
		if i == pbinRebuildCodeAccounts-1 {
			states[i].Nonce = 1
			states[i].Balance = *uint256.NewInt(uint64(i) + 9)
		}
	}
	return db, states
}

func pbinWriteCodeRange(t *testing.T, db kv.TemporalRwDB, from, count uint64, shape pbinRebuildCodeShape, cleared bool) {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(rebuildVariantTrieCfg(commitment.VariantHexPatriciaTrie)))
	require.NoError(t, err)
	defer sd.Close()
	sd.DiscardWrites(kv.CommitmentDomain)
	for i := range pbinRebuildCodeAccounts {
		address := pbinRebuildCodeAddress(shape, i)
		txNum := from + uint64(i)*count/pbinRebuildCodeAccounts
		if !cleared {
			if i == pbinRebuildCodeAccounts-1 {
				account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(uint64(i) + 9), CodeHash: accounts.EmptyCodeHash}
				prev, _, err := sd.GetLatest(kv.AccountsDomain, tx, address)
				require.NoError(t, err)
				require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), txNum, prev))
				continue
			}
			code := pbinRebuildCode(shape, address)
			account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(uint64(i) + 9), CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}
			prev, _, err := sd.GetLatest(kv.AccountsDomain, tx, address)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), txNum, prev))
			require.NoError(t, sd.DomainPut(kv.CodeDomain, tx, address, code, txNum, nil))
			continue
		}
		if i == pbinRebuildCodeAccounts-1 {
			continue
		}
		prev, _, err := sd.GetLatest(kv.AccountsDomain, tx, address)
		require.NoError(t, err)
		if shape == pbinRebuildContract {
			codeKey := address
			previousCode, _, codeErr := sd.GetLatest(kv.CodeDomain, tx, codeKey)
			require.NoError(t, codeErr)
			require.NoError(t, sd.DomainDel(kv.CodeDomain, tx, codeKey, txNum, previousCode))
			require.NoError(t, sd.DomainDel(kv.AccountsDomain, tx, address, txNum, prev))
			txNum++
		}
		account := accounts.Account{Nonce: 2, Balance: *uint256.NewInt(uint64(i) + 17), CodeHash: accounts.EmptyCodeHash}
		prev, _, err = sd.GetLatest(kv.AccountsDomain, tx, address)
		require.NoError(t, err)
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), txNum, prev))
	}
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
}

func pbinWriteCodeGuard(t *testing.T, db kv.TemporalRwDB, txNum uint64, shape pbinRebuildCodeShape) {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(rebuildVariantTrieCfg(commitment.VariantHexPatriciaTrie)))
	require.NoError(t, err)
	defer sd.Close()
	sd.DiscardWrites(kv.CommitmentDomain)
	address := pbinRebuildCodeAddress(shape, 0)
	account := accounts.Account{Nonce: 2, Balance: *uint256.NewInt(17), CodeHash: accounts.EmptyCodeHash}
	prev, _, err := sd.GetLatest(kv.AccountsDomain, tx, address)
	require.NoError(t, err)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), txNum, prev))
	address = pbinRebuildCodeAddress(shape, pbinRebuildCodeAccounts-1)
	account = accounts.Account{Nonce: 2, Balance: *uint256.NewInt(uint64(pbinRebuildCodeAccounts-1) + 17), CodeHash: accounts.EmptyCodeHash}
	prev, _, err = sd.GetLatest(kv.AccountsDomain, tx, address)
	require.NoError(t, err)
	require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), txNum, prev))
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
}

func pbinRebuildExpectedRoot(t *testing.T, shape pbinRebuildCodeShape, states []eip8297.State, hash string) common.Hash {
	t.Helper()
	previous := eip8297.HashSuiteName()
	require.NoError(t, eip8297.SetHashSuite(hash))
	t.Cleanup(func() { require.NoError(t, eip8297.SetHashSuite(previous)) })
	initial := make([]eip8297.State, len(states))
	for i := range states {
		initial[i] = states[i]
		initial[i].Nonce = 1
		initial[i].Balance = *uint256.NewInt(uint64(i) + 9)
		if i != len(initial)-1 {
			initial[i].Code = pbinRebuildCode(shape, initial[i].Address)
		}
	}
	return eip8297.StateRootWithHash(eip8297.EmbedState([][]eip8297.State{initial, states}), eip8297.SelectedHash())
}

func TestPBinRebuildRewritesCodelessCodeFieldsAcrossRanges(t *testing.T) {
	for _, shape := range []pbinRebuildCodeShape{pbinRebuildDelegation, pbinRebuildContract} {
		for _, hash := range []string{commitment.PBinHashKeccak, commitment.PBinHashBlake3} {
			t.Run(fmt.Sprintf("shape-%d/%s", shape, hash), func(t *testing.T) {
				db, states := pbinRebuildCodeDatadir(t, shape)
				root, report, err := state.RebuildCommitmentFiles(t.Context(), db, &rawdbv3.TxNums, log.New(), false,
					state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: hash})
				require.NoError(t, err)
				require.NotNil(t, report)
				require.GreaterOrEqual(t, len(report.Ranges), 2)
				require.Equal(t, pbinRebuildExpectedRoot(t, shape, states, hash), common.BytesToHash(root))
			})
		}
	}
}
