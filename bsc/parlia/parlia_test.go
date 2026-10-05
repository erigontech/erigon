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

package parlia

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/bsc/parlia/seal"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestSealHashUsesChainID(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	header := &types.Header{Number: *uint256.NewInt(40000000), Extra: make([]byte, 32+65)}

	want, err := seal.Hash(header, spec.Config.ChainID)
	require.NoError(t, err)
	require.Equal(t, want, New(spec.Config, log.New()).SealHash(header))
}

type parentReader struct {
	rules.ChainHeaderReader
	parent *types.Header
}

func (r parentReader) GetHeader(common.Hash, uint64) *types.Header { return r.parent }

// From Prague, Parlia stores the parent hash in the EIP-2935 history contract at
// the start of the block (BEP-440), but only once the contract is deployed.
// BSC has no deployment transaction for the EIP-2935 history contract: the
// first Prague block installs it before storing the parent hash.
func TestInitializeDeploysHistoryContractOnFirstPragueBlock(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	p := New(spec.Config, log.New())

	parentHash := common.HexToHash("0x696ff4c97416482d0dd9c9565c67e6e71b69f63faa32ada7250c8033dce61277")
	const forkBlock = 48576786
	pragueTime := *spec.Config.PragueTime
	header := &types.Header{Number: *uint256.NewInt(forkBlock), Time: pragueTime, ParentHash: parentHash}
	parent := &types.Header{Number: *uint256.NewInt(forkBlock - 1), Time: pragueTime - 3}

	ibs := state.New(state.NewNoopReader())
	require.NoError(t, p.Initialize(spec.Config, parentReader{parent: parent}, header, ibs, nil, log.New(), nil))

	code, err := ibs.GetCode(params.HistoryStorageAddress)
	require.NoError(t, err)
	require.Equal(t, hexutil.MustDecode("0x3373fffffffffffffffffffffffffffffffffffffffe14604657602036036042575f35600143038111604257611fff81430311604257611fff9006545f5260205ff35b5f5ffd5b5f35611fff60014303065500"), code)
	nonce, err := ibs.GetNonce(params.HistoryStorageAddress)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nonce)

	slot := accounts.InternKey(common.BytesToHash(uint256.NewInt((forkBlock - 1) % params.BlockHashHistoryServeWindow).Bytes()))
	got, err := ibs.GetState(params.HistoryStorageAddress, slot)
	require.NoError(t, err)
	require.Equal(t, parentHash, common.Hash(got.Bytes32()))
}

func TestInitializeStoresParentHashFromPrague(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	p := New(spec.Config, log.New())

	parentHash := common.HexToHash("0x696ff4c97416482d0dd9c9565c67e6e71b69f63faa32ada7250c8033dce61277")
	const pragueBlock, pragueTime = 49000000, 1741722894
	slot := accounts.InternKey(common.BytesToHash(uint256.NewInt((pragueBlock - 1) % params.BlockHashHistoryServeWindow).Bytes()))

	for _, tc := range []struct {
		name     string
		time     uint64
		deployed bool
		want     common.Hash
	}{
		{"prague", pragueTime, true, parentHash},
		{"prague without the contract", pragueTime, false, common.Hash{}},
		{"before prague", *spec.Config.PragueTime - 100, true, common.Hash{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			if tc.deployed {
				require.NoError(t, ibs.SetCode(params.HistoryStorageAddress, []byte{0x00}, tracing.CodeChangeUnspecified))
			}
			header := &types.Header{Number: *uint256.NewInt(pragueBlock), Time: tc.time, ParentHash: parentHash}
			parent := &types.Header{Number: *uint256.NewInt(pragueBlock - 1), Time: tc.time - 3}

			require.NoError(t, p.Initialize(spec.Config, parentReader{parent: parent}, header, ibs, nil, log.New(), nil))

			got, err := ibs.GetState(params.HistoryStorageAddress, slot)
			require.NoError(t, err)
			require.Equal(t, tc.want, common.Hash(got.Bytes32()))
		})
	}
}
