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
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type countingFrameValidationServer struct {
	remoteproto.ETHBACKENDServer
	calls int
}

func (s *countingFrameValidationServer) FrameValidation(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
	s.calls++
	return s.ETHBACKENDServer.FrameValidation(ctx, req)
}

func TestFramePoolConsumedNonceReply(t *testing.T) {
	cfg := chain.TestChainOsakaConfig.Copy()
	cfg.AmsterdamTime, cfg.BogotaTime = common.NewUint64(0), common.NewUint64(0)
	sender := common.HexToAddress("0x1000")
	genesis := &types.Genesis{Config: cfg, GasLimit: 30_000_000, Alloc: types.GenesisAlloc{
		sender: {Nonce: 1, Balance: big.NewInt(1_000_000_000_000_000_000), Code: []byte{0x60, 3, 0x5f, 0x5f, 0xaa}},
		types.FrameNonceManager: {Nonce: 1, Storage: map[common.Hash]common.Hash{
			types.FrameNonceSlot(sender, uint256.Int{1}): common.BigToHash(big.NewInt(1)),
		}},
	}}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis))
	for _, tc := range []struct {
		name string
		key  uint256.Int
	}{
		{name: "legacy"},
		{name: "keyed", key: uint256.Int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, logger := t.Context(), log.New()
			backend := &countingFrameValidationServer{ETHBACKENDServer: privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications, m.BlockReader, logger, nil, cfg)}
			poolDB := mdbxtest.NewTestPoolDB(t)
			pool, err := txpool.New(ctx, make(chan txpool.Announcements, 1), poolDB, m.DB, txpoolcfg.DefaultConfig,
				kvcache.NewLatestBatchCache(), cfg, nil, nil, func() {}, nil, direct.NewEthBackendClientDirect(backend), logger, txpool.WithFeeCalculator(nil))
			require.NoError(t, err)
			require.NoError(t, pool.OnNewBlock(ctx, &remoteproto.StateChangeBatch{
				BlockGasLimit: genesis.GasLimit,
				ChangeBatch:   []*remoteproto.StateChange{{BlockHash: gointerfaces.ConvertHashToH256(m.Genesis.Hash())}},
			}, txpool.TxnSlots{}, txpool.TxnSlots{}, txpool.TxnSlots{}))
			frame := &types.FrameTransaction{
				NonceKeys: []uint256.Int{tc.key}, ChainID: *cfg.ChainID, SenderAddress: sender,
				Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}},
				Fees:   types.FrameFees{FeeCap: *uint256.NewInt(10_000_000_000)},
			}
			var encoded bytes.Buffer
			require.NoError(t, frame.MarshalBinary(&encoded))
			server := txpool.NewGrpcServer(ctx, pool, poolDB, nil, *cfg.ChainID, logger)
			request := &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes(), encoded.Bytes()}}
			// The duplicate within the batch and a later submission reach different cache checks.
			for range 2 {
				reply, err := server.Add(ctx, request)
				require.NoError(t, err)
				require.Len(t, reply.Imported, len(request.RlpTxs))
				for i := range reply.Imported {
					require.Equal(t, txpoolproto.ImportResult_STALE, reply.Imported[i])
					require.Equal(t, txpoolcfg.NonceTooLow.String(), reply.Errors[i])
				}
				require.Equal(t, 1, backend.calls, "cached rejections must preserve the reason without another simulation")
				request.RlpTxs = request.RlpTxs[:1]
			}
			hash := frame.Hash()
			require.NotEqual(t, txpoolcfg.AlreadyKnown, pool.KnownTxnReason(hash[:]))
			require.NoError(t, poolDB.View(ctx, func(tx kv.Tx) error {
				known, err := pool.IdHashKnown(tx, hash[:])
				require.NoError(t, err)
				require.True(t, known, "rejected frames must still be deduplicated for gossip")
				return nil
			}))
		})
	}
}
