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

package receipts

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// DeriveFields must fill Bloom: finalize-produced and cache-read receipts
// arrive without one, and the notification path publishes the set as-is.
func TestDeriveFieldsSetsBloom(t *testing.T) {
	withLogs := &types.Receipt{
		Logs: []*types.Log{{
			Address: common.HexToAddress("0x01"),
			Topics:  []common.Hash{common.HexToHash("0x02")},
		}},
	}
	noLogs := &types.Receipt{}
	blockHash := common.HexToHash("0xabc")

	DeriveFields(types.Receipts{withLogs, noLogs}, blockHash)

	assert.NotEqual(t, types.Bloom{}, withLogs.Bloom)
	assert.Equal(t, types.CreateBloom(types.Receipts{withLogs}), withLogs.Bloom)
	assert.Equal(t, types.Bloom{}, noLogs.Bloom)
	assert.Equal(t, blockHash, withLogs.BlockHash)
	assert.Equal(t, blockHash, noLogs.BlockHash)
}

// Replay-produced receipts arrive with Bloom already computed; DeriveFields
// must reuse it instead of hashing the same logs again.
func TestDeriveFieldsPreservesExistingBloom(t *testing.T) {
	var preset types.Bloom
	preset[0] = 0x80
	preset[types.BloomByteLength-1] = 0x01
	withPresetBloom := &types.Receipt{
		Bloom: preset,
		Logs: []*types.Log{{
			Address: common.HexToAddress("0x01"),
			Topics:  []common.Hash{common.HexToHash("0x02")},
		}},
	}

	DeriveFields(types.Receipts{withPresetBloom}, common.HexToHash("0xabc"))

	assert.Equal(t, preset, withPresetBloom.Bloom)
}

type fundedReader struct {
	*state.NoopReader
	funded accounts.Address
}

func (r fundedReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr != r.funded {
		return nil, nil
	}
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1_000_000_000_000_000_000)
	return &acc, nil
}

// A later tx must see the earlier ones on a versioned state, whether they produce receipts or
// only warm the state up: the second transfer needs the first one's nonce.
func TestDeriveForRangeVersionedMatchesMaterialized(t *testing.T) {
	t.Parallel()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
	cfg := chain.TestChainBerlinConfig
	signer := types.LatestSignerForChainID(cfg.ChainID)
	to := common.HexToAddress("0x00000000000000000000000000000000c0ffee01")
	var txns types.Transactions
	for nonce := range uint64(2) {
		txn, err := types.SignTx(types.NewTransaction(nonce, to, uint256.NewInt(7), 21_000, uint256.NewInt(1), nil), *signer, key)
		require.NoError(t, err)
		txns = append(txns, txn)
	}
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 1_000_000}
	derive := func(ibs *state.IntraBlockState, fromIdx int) types.Receipts {
		defer ibs.Close()
		gp := new(protocol.GasPool).AddGas(header.GasLimit)
		receipts, err := DeriveForRange(context.Background(), cfg, ethash.NewFaker(), header, txns, fromIdx, len(txns), ibs, gp, nil)
		require.NoError(t, err)
		return receipts
	}
	reader := fundedReader{NoopReader: state.NewNoopReader(), funded: sender}
	for _, fromIdx := range []int{0, 1} {
		want := derive(state.New(reader), fromIdx)
		got := derive(state.NewWithVersionMap(reader, state.NewVersionMap(nil)), fromIdx)
		require.Len(t, got, len(txns)-fromIdx)
		require.Equal(t, want, got, "fromIdx=%d", fromIdx)
	}
}
