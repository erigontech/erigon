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

package engineapi

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func TestGetPayloadV4RejectsNilRequests(t *testing.T) {
	t.Parallel()

	const payloadID uint64 = 42
	stub := &stubExecutionServer{
		getAssembledBlockFunc: func(_ context.Context, req *executionproto.GetAssembledBlockRequest) (*executionproto.GetAssembledBlockResponse, error) {
			require.Equal(t, payloadID, req.Id)
			return &executionproto.GetAssembledBlockResponse{
				Busy: false,
				Data: assembledBlockDataForGetPayloadV4(1, nil),
			}, nil
		},
	}

	srv := newProposingEngineServerForGetPayloadTests(stub)
	resp, err := srv.GetPayloadV4(context.Background(), payloadIDBytes(payloadID))

	require.Nil(t, resp)
	require.ErrorContains(t, err, "missing execution requests")
}

func TestGetPayloadV4AcceptsEmptyRequestsBundle(t *testing.T) {
	t.Parallel()

	const payloadID uint64 = 43
	stub := &stubExecutionServer{
		getAssembledBlockFunc: func(_ context.Context, req *executionproto.GetAssembledBlockRequest) (*executionproto.GetAssembledBlockResponse, error) {
			require.Equal(t, payloadID, req.Id)
			return &executionproto.GetAssembledBlockResponse{
				Busy: false,
				Data: assembledBlockDataForGetPayloadV4(1, &typesproto.RequestsBundle{
					Requests: make([][]byte, 0),
				}),
			}, nil
		},
	}

	srv := newProposingEngineServerForGetPayloadTests(stub)
	resp, err := srv.GetPayloadV4(context.Background(), payloadIDBytes(payloadID))

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.ExecutionRequests)
	require.Len(t, resp.ExecutionRequests, 0)
}

func newProposingEngineServerForGetPayloadTests(stub *stubExecutionServer) *EngineServer {
	cfg := allForksChainConfig()
	// GetPayloadV4 is valid on Prague but invalid once Osaka activates.
	cfg.OsakaTime = nil
	cfg.AmsterdamTime = nil

	return NewEngineServer(
		log.New(),
		cfg,
		direct.NewExecutionClientDirect(stub),
		nil,   // blockDownloader
		false, // caplin
		false, // internalCL
		true,  // proposing
		true,  // consuming
		nil,   // txPool
		0,     // fcuTimeout
		0,     // maxReorgDepth
	)
}

func payloadIDBytes(payloadID uint64) hexutil.Bytes {
	payloadBytes := make(hexutil.Bytes, 8)
	binary.BigEndian.PutUint64(payloadBytes, payloadID)
	return payloadBytes
}

func assembledBlockDataForGetPayloadV4(timestamp uint64, requests *typesproto.RequestsBundle) *executionproto.AssembledBlockData {
	return &executionproto.AssembledBlockData{
		ExecutionPayload: &typesproto.ExecutionPayload{
			Version:       4, // Prague+
			ParentHash:    gointerfaces.ConvertHashToH256(common.Hash{0x01}),
			Coinbase:      gointerfaces.ConvertAddressToH160(common.Address{}),
			StateRoot:     gointerfaces.ConvertHashToH256(common.Hash{0x02}),
			ReceiptRoot:   gointerfaces.ConvertHashToH256(common.Hash{0x03}),
			LogsBloom:     gointerfaces.ConvertBytesToH2048(make([]byte, 256)),
			PrevRandao:    gointerfaces.ConvertHashToH256(common.Hash{0x04}),
			BlockNumber:   101,
			GasLimit:      30_000_000,
			GasUsed:       21_000,
			Timestamp:     timestamp,
			ExtraData:     []byte("test"),
			BaseFeePerGas: gointerfaces.ConvertUint256IntToH256(uint256.NewInt(1_000_000_000)),
			BlockHash:     gointerfaces.ConvertHashToH256(common.Hash{0x05}),
			Transactions:  make([][]byte, 0),
			BlobGasUsed:   ptrUint64(0),
			ExcessBlobGas: ptrUint64(0),
		},
		BlockValue: gointerfaces.ConvertUint256IntToH256(uint256.NewInt(0)),
		Requests:   requests,
	}
}
