// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"context"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReproduceCrash pins that setting two storage keys non-zero, then clearing both in the same block, does not crash.
func TestReproduceCrash(t *testing.T) {
	t.Parallel()
	value0 := uint256.NewInt(0)
	contract := accounts.InternAddress(common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624"))
	storageKey1 := accounts.InternKey(common.HexToHash("0x0e4c0e7175f9d22279a4f63ff74f7fa28b7a954a6454debaa62ce43dd9132541"))
	value1 := uint256.NewInt(0x016345785d8a0000)
	storageKey2 := accounts.InternKey(common.HexToHash("0x0e4c0e7175f9d22279a4f63ff74f7fa28b7a954a6454debaa62ce43dd9132542"))
	value2 := uint256.NewInt(0x58c00a51)

	_, tx, sd := state.NewTestRwTx(t)

	txNum := uint64(1)
	tsw := state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	tsr := state.NewReaderV3(sd.AsGetter(tx))

	intraBlockState := state.New(tsr)
	defer intraBlockState.Close()
	intraBlockState.CreateAccount(contract, true)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	intraBlockState.SetState(contract, storageKey1, *value1)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	intraBlockState.AddBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	intraBlockState.SetState(contract, storageKey2, *value2)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	intraBlockState.SubBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	intraBlockState.SetState(contract, storageKey1, *value0)
	intraBlockState.SetState(contract, storageKey2, *value0)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
}

func TestChangeAccountCodeBetweenBlocks(t *testing.T) {
	t.Parallel()
	contract := accounts.InternAddress(common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624"))

	_, tx, sd := state.NewTestRwTx(t)
	blockNum, txNum := uint64(1), uint64(3)
	_ = blockNum

	r, tsw := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	intraBlockState := state.New(r)
	defer intraBlockState.Close()
	intraBlockState.CreateAccount(contract, true)

	oldCode := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, oldCode, tracing.CodeChangeUnspecified)
	intraBlockState.AddBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	rh1, err := sd.ComputeCommitment(context.Background(), tx, true, blockNum, txNum, "", nil)
	require.NoError(t, err)

	trieCode, tcErr := r.ReadAccountCode(contract)
	require.NoError(t, tcErr, "you can receive the new code")
	assert.Equal(t, oldCode, trieCode, "new code should be received")

	newCode := []byte{0x04, 0x04, 0x04, 0x04}
	intraBlockState.SetCode(contract, newCode, tracing.CodeChangeUnspecified)

	if err := intraBlockState.FinalizeTx(&chain.Rules{}, tsw); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}

	trieCode, tcErr = r.ReadAccountCode(contract)
	require.NoError(t, tcErr, "you can receive the new code")
	assert.Equal(t, newCode, trieCode, "new code should be received")

	rh2, err := sd.ComputeCommitment(context.Background(), tx, true, blockNum, txNum, "", nil)
	require.NoError(t, err)
	require.NotEqual(t, rh1, rh2)
}

// TestCacheCodeSizeSeparately makes sure that we don't store CodeNodes for code sizes
func TestCacheCodeSizeSeparately(t *testing.T) {
	t.Parallel()
	contract := accounts.InternAddress(common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624"))
	_, tx, sd := state.NewTestRwTx(t)
	blockNum, txNum := uint64(1), uint64(3)
	_ = blockNum

	r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)

	intraBlockState := state.New(r)
	defer intraBlockState.Close()
	intraBlockState.CreateAccount(contract, true)

	code := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, code, tracing.CodeChangeUnspecified)
	intraBlockState.AddBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, w); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if err := intraBlockState.CommitBlock(&chain.Rules{}, w); err != nil {
		t.Errorf("error committing block: %v", err)
	}

	codeSize, err := r.ReadAccountCodeSize(contract)
	require.NoError(t, err, "you can receive the new code")
	assert.Equal(t, len(code), codeSize, "new code should be received")

	code2, err := r.ReadAccountCode(contract)
	require.NoError(t, err, "you can receive the new code")
	assert.Equal(t, code, code2, "new code should be received")
}

// TestCacheCodeSizeInTrie makes sure that we don't just read from the DB all the time
func TestCacheCodeSizeInTrie(t *testing.T) {
	t.Parallel()
	contract := accounts.InternAddress(common.HexToAddress("0x71dd1027069078091B3ca48093B00E4735B20624"))
	root := common.HexToHash("0xb939e5bcf5809adfb87ab07f0795b05b95a1d64a90f0eddd0c3123ac5b433854")

	_, tx, sd := state.NewTestRwTx(t)
	blockNum := uint64(1)
	txNum := uint64(3)

	r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)

	intraBlockState := state.New(r)
	defer intraBlockState.Close()
	intraBlockState.CreateAccount(contract, true)

	code := []byte{0x01, 0x02, 0x03, 0x04}

	intraBlockState.SetCode(contract, code, tracing.CodeChangeUnspecified)
	intraBlockState.AddBalance(contract, *uint256.NewInt(1000000000), tracing.BalanceChangeUnspecified)
	if err := intraBlockState.FinalizeTx(&chain.Rules{}, w); err != nil {
		t.Errorf("error finalising 1st tx: %v", err)
	}
	if err := intraBlockState.CommitBlock(&chain.Rules{}, w); err != nil {
		t.Errorf("error committing block: %v", err)
	}

	r2, err := sd.ComputeCommitment(context.Background(), tx, true, blockNum, txNum, "", nil)
	require.NoError(t, err)
	require.Equal(t, root, common.CastToHash(r2))

	codeSize, err := r.ReadAccountCodeSize(contract)
	require.NoError(t, err, "you can receive the code size ")
	assert.Equal(t, len(code), codeSize, "you can receive the code size")

	codeSize2, err := r.ReadAccountCodeSize(contract)
	require.NoError(t, err, "you can still receive code size even with empty DB")
	assert.Equal(t, len(code), codeSize2, "code size should be received even with empty DB")

	r2, err = sd.ComputeCommitment(context.Background(), tx, true, 1, 2, "", nil)
	require.NoError(t, err)
	require.Equal(t, root, common.CastToHash(r2))
}
