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

package engine_types

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestBlobsBundleMarshalFastJSONMatchesReflection(t *testing.T) {
	cases := map[string]*BlobsBundle{
		"nil bundle":   nil,
		"empty struct": {},
		"empty arrays": {Commitments: []hexutil.Bytes{}, Proofs: []hexutil.Bytes{}, Blobs: []hexutil.Bytes{}},
		"populated":    {Commitments: []hexutil.Bytes{{0x01, 0x02}}, Proofs: []hexutil.Bytes{{0x03}, {0x04}}, Blobs: []hexutil.Bytes{{0x05}}},
		"worst case":   worstCaseBlobsBundle(),
	}
	for name, bundle := range cases {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal(bundle)
			require.NoError(t, err)
			got, err := jsonstream.Marshal(bundle)
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}

func TestGetPayloadResponseMarshalFastJSONMatchesReflection(t *testing.T) {
	cases := map[string]*GetPayloadResponse{
		"nil bundle": {
			BlockValue:        (*hexutil.U256)(uint256.NewInt(123)),
			ExecutionRequests: []hexutil.Bytes{{0x01}, {0x02}},
		},
		"empty-array bundle": {
			BlobsBundle: &BlobsBundle{Commitments: []hexutil.Bytes{}, Proofs: []hexutil.Bytes{}, Blobs: []hexutil.Bytes{}},
		},
		"small bundle": {
			BlockValue:            (*hexutil.U256)(uint256.NewInt(7)),
			BlobsBundle:           &BlobsBundle{Commitments: []hexutil.Bytes{{0x01}}, Proofs: []hexutil.Bytes{{0x02}, {0x03}}, Blobs: []hexutil.Bytes{{0x04}}},
			ShouldOverrideBuilder: true,
		},
		"worst-case bundle": {
			BlobsBundle: worstCaseBlobsBundle(),
		},
		"nil response": nil,
		"populated payload": {
			ExecutionPayload: &ExecutionPayload{
				ParentHash:    common.HexToHash("0xabc1"),
				FeeRecipient:  common.HexToAddress("0xfee"),
				StateRoot:     common.HexToHash("0x5747"),
				BlockNumber:   2,
				GasLimit:      30_000_000,
				GasUsed:       21_000,
				Timestamp:     1_700_000_000,
				BaseFeePerGas: (*hexutil.U256)(uint256.NewInt(1_000_000_000)),
				BlockHash:     common.HexToHash("0xb10c"),
				Transactions:  []hexutil.Bytes{{0x01, 0x02}, {0x03}},
			},
			BlockValue:  (*hexutil.U256)(uint256.NewInt(99)),
			BlobsBundle: &BlobsBundle{Commitments: []hexutil.Bytes{{0x01}}, Proofs: []hexutil.Bytes{{0x02}}, Blobs: []hexutil.Bytes{{0x03}}},
		},
	}
	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			want, err := json.Marshal(r)
			require.NoError(t, err)
			got, err := jsonstream.Marshal(r)
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}

func worstCaseBlobsBundle() *BlobsBundle {
	// worst case for getPayload: a full mainnet block (21 blobs) with Osaka cell proofs (128/blob).
	const blobs = 21
	bundle := &BlobsBundle{
		Commitments: make([]hexutil.Bytes, blobs),
		Proofs:      make([]hexutil.Bytes, blobs*sszCellsPerExtBlob),
		Blobs:       make([]hexutil.Bytes, blobs),
	}
	for i := range bundle.Blobs {
		blob := make(hexutil.Bytes, sszBlobBytes)
		for j := range blob {
			blob[j] = byte(i + j)
		}
		bundle.Blobs[i] = blob
		commitment := make(hexutil.Bytes, sszKZGBytes)
		for j := range commitment {
			commitment[j] = byte(i + j)
		}
		bundle.Commitments[i] = commitment
	}
	for i := range bundle.Proofs {
		proof := make(hexutil.Bytes, sszKZGBytes)
		for j := range proof {
			proof[j] = byte(i + j)
		}
		bundle.Proofs[i] = proof
	}
	return bundle
}

func TestGetPayloadResponseQuantitiesJSON(t *testing.T) {
	for _, q := range []string{`"0x0"`, `"0x3b9aca00"`, `"0x8000000000000000000000000000000000000000000000000000000000000000"`, `null`} {
		enc, err := json.Marshal(getPayloadResponse(t, q, q, 2))
		require.NoError(t, err)
		require.Contains(t, string(enc), `"baseFeePerGas":`+q)
		require.Contains(t, string(enc), `"blockValue":`+q)

		var r GetPayloadResponse
		require.NoError(t, json.Unmarshal(enc, &r))
		again, err := json.Marshal(&r)
		require.NoError(t, err)
		require.Equal(t, string(enc), string(again), q)
	}
}

// The payload bodies are streamed field by field, so they must match encoding/json byte for byte:
// missing blocks are null elements, and nil withdrawals or access list are null, not [].
func TestExecutionPayloadBodiesMarshalFastJSONToMatchesReflection(t *testing.T) {
	txs := []hexutil.Bytes{{0x02, 0xab}, {0xf8, 0x01}, {}}
	withdrawals := []*types.Withdrawal{{Index: 0, Validator: 1, Address: common.Address{0xaa}, Amount: 0}, nil, {Index: 7, Validator: 8, Address: common.Address{0xbb}, Amount: 9}}
	bal := hexutil.Bytes{0xc0}
	emptyBal := hexutil.Bytes{}
	for name, bodies := range map[string]ExecutionPayloadBodies{
		"nil":               nil,
		"empty":             {},
		"missing block":     {nil, {Transactions: txs}},
		"nil withdrawals":   {{Transactions: txs}},
		"empty withdrawals": {{Transactions: []hexutil.Bytes{}, Withdrawals: []*types.Withdrawal{}}},
		"withdrawals":       {{Transactions: txs, Withdrawals: withdrawals}},
		"nil tx list":       {{Withdrawals: withdrawals}},
	} {
		t.Run("v1 "+name, func(t *testing.T) { requireStreamedMatchesReflection(t, bodies) })
	}
	for name, bodies := range map[string]ExecutionPayloadBodiesV2{
		"nil":               nil,
		"missing block":     {nil},
		"nil access list":   {{Transactions: txs, Withdrawals: withdrawals}},
		"access list":       {{Transactions: txs, BlockAccessList: &bal}},
		"empty access list": {{Transactions: txs, Withdrawals: []*types.Withdrawal{}, BlockAccessList: &emptyBal}},
	} {
		t.Run("v2 "+name, func(t *testing.T) { requireStreamedMatchesReflection(t, bodies) })
	}
}

func requireStreamedMatchesReflection(t *testing.T, v interface {
	MarshalFastJSONTo(*jsonstream.StackStream) error
}) {
	t.Helper()
	want, err := json.Marshal(v)
	require.NoError(t, err)
	s := jsonstream.Get(nil)
	defer jsonstream.Put(s)
	require.NoError(t, v.MarshalFastJSONTo(s))
	require.NoError(t, s.Flush())
	require.Equal(t, string(want), string(s.Buffer()))
}
