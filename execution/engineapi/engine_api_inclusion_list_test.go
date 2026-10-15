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
	"encoding/json"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
)

const ilTestMaxReorgDepth = 64

func bogotaChainConfig() *chain.Config {
	cfg := allForksChainConfig()
	cfg.BogotaTime = common.NewUint64(0)
	return cfg
}

// bogotaPayload returns an empty ExecutionPayloadV4 on top of parent whose
// blockHash matches the header newPayload rebuilds from it.
func bogotaPayload(t *testing.T, parent *types.Header) *engine_types.ExecutionPayload {
	t.Helper()
	balBytes, err := types.EncodeBlockAccessListBytes(types.BlockAccessList{})
	require.NoError(t, err)
	bal, err := types.DecodeBlockAccessListSidecar(balBytes)
	require.NoError(t, err)
	balHash, err := bal.Hash()
	require.NoError(t, err)

	var zero uint64
	slotNumber := uint64(1)
	withdrawalsHash := types.DeriveSha(types.Withdrawals{})
	header := &types.Header{
		ParentHash:            parent.Hash(),
		UncleHash:             empty.UncleHash,
		Difficulty:            *merge.ProofOfStakeDifficulty,
		Nonce:                 merge.ProofOfStakeNonce,
		TxHash:                types.DeriveSha(types.BinaryTransactions{}),
		GasLimit:              parent.GasLimit,
		Time:                  parent.Time + 12,
		BaseFee:               uint256.NewInt(1_000_000_000),
		WithdrawalsHash:       &withdrawalsHash,
		RequestsHash:          types.FlatRequests{}.Hash(),
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
		ParentBeaconBlockRoot: &common.Hash{},
		BlockAccessListHash:   &balHash,
		SlotNumber:            &slotNumber,
	}
	header.Number.SetUint64(parent.Number.Uint64() + 1)

	blobGas := hexutil.Uint64(0)
	slot := hexutil.Uint64(slotNumber)
	balParam := hexutil.Bytes(balBytes)
	return &engine_types.ExecutionPayload{
		ParentHash:      header.ParentHash,
		LogsBloom:       make(hexutil.Bytes, types.BloomByteLength),
		BlockNumber:     hexutil.Uint64(header.Number.Uint64()),
		GasLimit:        hexutil.Uint64(header.GasLimit),
		Timestamp:       hexutil.Uint64(header.Time),
		BaseFeePerGas:   (*hexutil.U256)(header.BaseFee),
		BlockHash:       header.Hash(),
		Transactions:    []hexutil.Bytes{},
		Withdrawals:     []*types.Withdrawal{},
		BlobGasUsed:     &blobGas,
		ExcessBlobGas:   &blobGas,
		SlotNumber:      &slot,
		BlockAccessList: &balParam,
	}
}

func signedInclusionList(t *testing.T, cfg *chain.Config) []hexutil.Bytes {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return []hexutil.Bytes{makeSignedRawTx(t, key, cfg, 0, 101, 1012)}
}

// newInclusionListServer wires an EngineServer whose execution module knows
// parent, accepts any inserted block and answers ValidateChain with validation.
func newInclusionListServer(cfg *chain.Config, parent *types.Header, validation execmodule.ValidationResult, maxReorgDepth uint64) (*EngineServer, *[]*types.Block) {
	var inserted []*types.Block
	stub := &stubExecutionModule{
		getHeaderFunc: getHeaderReturning(parent.Hash(), parent),
		currentHeaderFunc: func(context.Context) (*types.Header, error) {
			return parent, nil
		},
		insertBlocksFunc: func(_ context.Context, blocks []*types.Block) (execmodule.ExecutionStatus, error) {
			inserted = append(inserted, blocks...)
			return execmodule.ExecutionStatusSuccess, nil
		},
		validateChainFunc: func(context.Context, common.Hash, uint64) (execmodule.ValidationResult, error) {
			return validation, nil
		},
	}
	downloader := engine_block_downloader.NewEngineBlockDownloader(context.Background(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, maxReorgDepth)
	srv.test = true
	return srv, &inserted
}

func TestNewPayloadV6InclusionListSatisfied(t *testing.T) {
	t.Parallel()

	for _, satisfied := range []bool{true, false} {
		t.Run(map[bool]string{true: "satisfied", false: "unsatisfied"}[satisfied], func(t *testing.T) {
			t.Parallel()

			cfg := bogotaChainConfig()
			parent := makeParentHeader(1000)
			srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
				ValidationStatus:       execmodule.ExecutionStatusSuccess,
				LatestValidHash:        common.Hash{0x1},
				InclusionListSatisfied: &satisfied,
			}, ilTestMaxReorgDepth)

			status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
			require.NoError(t, err)
			require.NotNil(t, status)
			require.Equal(t, engine_types.ValidStatus, status.Status)
			require.NotNil(t, status.InclusionListSatisfied)
			require.Equal(t, satisfied, *status.InclusionListSatisfied)
		})
	}
}

func TestNewPayloadV6ValidWithoutInclusionListResultIsSatisfied(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
		LatestValidHash:  common.Hash{0x1},
	}, ilTestMaxReorgDepth)

	status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.NotNil(t, status.InclusionListSatisfied)
	require.True(t, *status.InclusionListSatisfied)
}

func TestNewPayloadV6InclusionListSatisfiedNullUnlessValid(t *testing.T) {
	t.Parallel()

	satisfied := true
	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	unknownParent := makeParentHeader(2000)

	for _, tc := range []struct {
		name          string
		parent        *types.Header
		validation    execmodule.ValidationResult
		maxReorgDepth uint64
		want          engine_types.EngineStatus
	}{
		{
			name:   "invalid",
			parent: parent,
			validation: execmodule.ValidationResult{
				ValidationStatus:       execmodule.ExecutionStatusBadBlock,
				LatestValidHash:        parent.Hash(),
				ValidationError:        "bad block",
				InclusionListSatisfied: &satisfied,
			},
			maxReorgDepth: ilTestMaxReorgDepth,
			want:          engine_types.InvalidStatus,
		},
		{
			name:          "accepted",
			parent:        parent,
			maxReorgDepth: 1,
			want:          engine_types.AcceptedStatus,
		},
		{
			name:          "syncing",
			parent:        unknownParent,
			maxReorgDepth: ilTestMaxReorgDepth,
			want:          engine_types.SyncingStatus,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv, _ := newInclusionListServer(cfg, tc.parent, tc.validation, tc.maxReorgDepth)

			status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
			require.NoError(t, err)
			require.NotNil(t, status)
			require.Equal(t, tc.want, status.Status)
			require.Nil(t, status.InclusionListSatisfied)
		})
	}
}

func TestNewPayloadV6InvalidBlockHashReturnsPayloadStatusV2(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{}, ilTestMaxReorgDepth)
	payload := bogotaPayload(t, parent)
	payload.BlockHash = common.Hash{0xde, 0xad}

	status, err := srv.NewPayloadV6(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.InvalidStatus, status.Status)
	require.Nil(t, status.InclusionListSatisfied)
}

func TestNewPayloadV6ForwardsInclusionListToExecution(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, inserted := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
	}, ilTestMaxReorgDepth)
	inclusionList := signedInclusionList(t, cfg)

	_, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, inclusionList)
	require.NoError(t, err)
	require.Len(t, *inserted, 1)
	il := (*inserted)[0].InclusionList()
	require.Len(t, il, len(inclusionList))
	want, err := types.DecodeTransactions([][]byte{inclusionList[0]})
	require.NoError(t, err)
	require.Equal(t, want[0].Hash(), il[0].Hash())
}

func TestNewPayloadV6AcceptsEmptyInclusionList(t *testing.T) {
	t.Parallel()

	satisfied := true
	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus:       execmodule.ExecutionStatusSuccess,
		InclusionListSatisfied: &satisfied,
	}, ilTestMaxReorgDepth)

	status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, []hexutil.Bytes{})
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.ValidStatus, status.Status)
}

func TestPayloadStatusV2MarshalsInclusionListSatisfied(t *testing.T) {
	t.Parallel()

	satisfied := false
	for _, tc := range []struct {
		name   string
		status engine_types.PayloadStatusV2
		want   string
	}{
		{"null", engine_types.PayloadStatusV2{Status: engine_types.SyncingStatus}, `null`},
		{"false", engine_types.PayloadStatusV2{Status: engine_types.ValidStatus, InclusionListSatisfied: &satisfied}, `false`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(tc.status)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			require.Contains(t, fields, "inclusionListSatisfied")
			require.JSONEq(t, tc.want, string(fields["inclusionListSatisfied"]))
		})
	}
}

func TestNewPayloadV6JSONRPC(t *testing.T) {
	t.Parallel()

	satisfied := true
	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus:       execmodule.ExecutionStatusSuccess,
		InclusionListSatisfied: &satisfied,
	}, ilTestMaxReorgDepth)
	client := newEngineInProcClient(t, srv)

	var status engine_types.PayloadStatusV2
	require.NoError(t, client.CallContext(t.Context(), &status, "engine_newPayloadV6",
		bogotaPayload(t, parent), []common.Hash{}, common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg)))
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.NotNil(t, status.InclusionListSatisfied)
	require.True(t, *status.InclusionListSatisfied)
}

func TestNewPayloadV6JSONRPCClient(t *testing.T) {
	t.Parallel()

	satisfied := true
	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, inserted := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus:       execmodule.ExecutionStatusSuccess,
		InclusionListSatisfied: &satisfied,
	}, ilTestMaxReorgDepth)
	client := &JsonRpcClient{rpcClient: newEngineInProcClient(t, srv)}
	inclusionList := signedInclusionList(t, cfg)

	status, err := client.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, inclusionList)
	require.NoError(t, err)
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.NotNil(t, status.InclusionListSatisfied)
	require.True(t, *status.InclusionListSatisfied)
	require.Len(t, *inserted, 1)
	require.Len(t, (*inserted)[0].InclusionList(), len(inclusionList))
}

func TestGetInclusionListV1PropagatesExecutionError(t *testing.T) {
	t.Parallel()

	client := newGetInclusionListClient(t, func(context.Context) (types.Transactions, error) {
		return nil, errors.New("inclusion list building is not available")
	})

	var result []hexutil.Bytes
	err := client.CallContext(t.Context(), &result, "engine_getInclusionListV1")
	require.ErrorContains(t, err, "inclusion list building is not available")
}
