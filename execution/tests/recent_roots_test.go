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

package executiontests

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/privateapi"
)

func TestFrameValidationRecentRootPendingSlot(t *testing.T) {
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	key := common.HexToHash("0x5f027aa1cbe2df279bf6518edd4b44ea5409fd800189ec35224e10ab05e574c3")
	entry := common.HexToHash("0x0a0d1254c851be5a133b4c9a9e300f5602fc0f43dbe65aa6a66930d4ca0a51b8")
	data := common.FromHex("b9382d35273c75a50631a3e84d3c75ec9266e2b18c35a627e16cdbf26a18ca8500000000000000010000000000000000000000000000000000000000000000000000000000000002")
	genesis := &types.Genesis{
		Config: cfg, Timestamp: uint64(time.Now().Add(-48 * time.Hour).Unix()), SlotNumber: common.NewUint64(1), GasLimit: 30_000_000,
		Alloc: types.GenesisAlloc{
			sender:                        {Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
			types.FrameRecentRootVerifier: {Code: protocol.RecentRootCode, Storage: map[common.Hash]common.Hash{key: entry}},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	frame := &types.FrameTransaction{
		NonceKeys: []uint256.Int{{}}, ChainID: *cfg.ChainID, SenderAddress: sender,
		Frames: []types.Frame{
			{Mode: types.FrameVerify, Target: &types.FrameRecentRootVerifier, Data: data, Limits: mdgas.MdGas{Execution: 20_000}},
			{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}},
		},
		Fees: types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000)},
	}
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	server := privateapi.NewEthBackendServer(t.Context(), nil, m.DB, m.Notifications, m.BlockReader, log.New(), nil, cfg)
	result, err := server.FrameValidation(t.Context(), &remoteproto.FrameValidationRequest{Transaction: encoded.Bytes()})
	require.NoError(t, err)
	require.True(t, result.Valid, "use the canonical head slot plus one, independently of wall-clock time")
}
