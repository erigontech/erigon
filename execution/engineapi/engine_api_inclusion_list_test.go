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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
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
	"github.com/erigontech/erigon/execution/rlp"
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

// preBogotaPayload returns an empty payload on top of parent carrying the
// fields engine_newPayload requires for version, from Bellatrix up to Gloas.
func preBogotaPayload(t *testing.T, parent *types.Header, version clparams.StateVersion) *engine_types.ExecutionPayload {
	t.Helper()
	header := &types.Header{
		ParentHash: parent.Hash(),
		UncleHash:  empty.UncleHash,
		Difficulty: *merge.ProofOfStakeDifficulty,
		Nonce:      merge.ProofOfStakeNonce,
		TxHash:     types.DeriveSha(types.BinaryTransactions{}),
		GasLimit:   parent.GasLimit,
		Time:       parent.Time + 12,
		BaseFee:    uint256.NewInt(1_000_000_000),
	}
	header.Number.SetUint64(parent.Number.Uint64() + 1)
	var withdrawals types.Withdrawals
	if version >= clparams.CapellaVersion {
		withdrawals = types.Withdrawals{}
		withdrawalsHash := types.DeriveSha(withdrawals)
		header.WithdrawalsHash = &withdrawalsHash
	}
	if version >= clparams.DenebVersion {
		header.BlobGasUsed = new(uint64)
		header.ExcessBlobGas = new(uint64)
		header.ParentBeaconBlockRoot = &common.Hash{}
	}
	if version >= clparams.ElectraVersion {
		header.RequestsHash = types.FlatRequests{}.Hash()
	}
	if version >= clparams.GloasVersion {
		balHash := empty.BlockAccessListHash
		header.BlockAccessListHash = &balHash
		header.SlotNumber = new(uint64)
	}
	payload, err := engine_types.ExecutionPayloadFromBlock(types.NewBlockFromStorage(header.Hash(), header, nil, nil, withdrawals, nil))
	require.NoError(t, err)
	if version >= clparams.GloasVersion {
		payload.BlockAccessList = &hexutil.Bytes{0xc0}
	}
	return payload
}

func signedInclusionList(t *testing.T, cfg *chain.Config) []hexutil.Bytes {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return []hexutil.Bytes{makeSignedRawTx(t, key, cfg, 0, 101, 1012)}
}

// newInclusionListServer wires an EngineServer whose execution module knows
// parent, accepts any inserted block, answers ValidateChain with validation and
// InclusionListSatisfied with ilSatisfied.
func newInclusionListServer(cfg *chain.Config, parent *types.Header, validation execmodule.ValidationResult, ilSatisfied *bool, maxReorgDepth uint64) (*EngineServer, *[]*types.Block) {
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
		ilSatisfiedFunc: func(context.Context, common.Hash, uint64, types.Transactions) (*bool, error) {
			return ilSatisfied, nil
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
				ValidationStatus: execmodule.ExecutionStatusSuccess,
				LatestValidHash:  common.Hash{0x1},
			}, &satisfied, ilTestMaxReorgDepth)

			status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
			require.NoError(t, err)
			require.NotNil(t, status)
			require.Equal(t, engine_types.ValidStatus, status.Status)
			require.NotNil(t, status.InclusionListSatisfied)
			require.Equal(t, satisfied, *status.InclusionListSatisfied)
		})
	}
}

func TestNewPayloadV6ValidWithoutInclusionListResultIsSyncing(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
		LatestValidHash:  common.Hash{0x1},
	}, nil, ilTestMaxReorgDepth)

	status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.SyncingStatus, status.Status)
	require.Nil(t, status.LatestValidHash)
	require.Nil(t, status.InclusionListSatisfied)
}

func TestNewPayloadV6EmptyInclusionListWithoutResultIsSatisfied(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
		LatestValidHash:  common.Hash{0x1},
	}, nil, ilTestMaxReorgDepth)

	status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, []hexutil.Bytes{})
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.NotNil(t, status.InclusionListSatisfied)
	require.True(t, *status.InclusionListSatisfied)
}

func TestNewPayloadV6ValidWithoutInclusionListResultAsksExecution(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	payload := bogotaPayload(t, parent)
	satisfied := false
	var asked common.Hash
	stub := &stubExecutionModule{
		getHeaderFunc: getHeaderReturning(parent.Hash(), parent),
		currentHeaderFunc: func(context.Context) (*types.Header, error) {
			return parent, nil
		},
		validateChainFunc: func(context.Context, common.Hash, uint64) (execmodule.ValidationResult, error) {
			return execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusSuccess, LatestValidHash: common.Hash{0x1}}, nil
		},
		ilSatisfiedFunc: func(_ context.Context, blockHash common.Hash, _ uint64, _ types.Transactions) (*bool, error) {
			asked = blockHash
			return &satisfied, nil
		},
	}
	downloader := engine_block_downloader.NewEngineBlockDownloader(context.Background(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, ilTestMaxReorgDepth)
	srv.test = true

	status, err := srv.NewPayloadV6(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, payload.BlockHash, asked)
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.NotNil(t, status.InclusionListSatisfied)
	require.False(t, *status.InclusionListSatisfied)
}

func TestNewPayloadV6KnownCanonicalBlockAsksExecution(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	payload := bogotaPayload(t, parent)
	satisfied := false
	var askedHash common.Hash
	stub := &stubExecutionModule{
		getHeaderFunc: func(_ context.Context, blockHash *common.Hash, _ *uint64) (*types.Header, error) {
			if blockHash != nil && (*blockHash == parent.Hash() || *blockHash == payload.BlockHash) {
				return parent, nil
			}
			return nil, nil
		},
		isCanonicalHashFunc: func(_ context.Context, hash common.Hash) (bool, error) {
			return hash == payload.BlockHash, nil
		},
		ilSatisfiedFunc: func(_ context.Context, blockHash common.Hash, _ uint64, _ types.Transactions) (*bool, error) {
			askedHash = blockHash
			return &satisfied, nil
		},
	}
	downloader := engine_block_downloader.NewEngineBlockDownloader(context.Background(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, ilTestMaxReorgDepth)
	srv.test = true

	status, err := srv.NewPayloadV6(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.NotNil(t, status.InclusionListSatisfied)
	require.False(t, *status.InclusionListSatisfied)
	require.Equal(t, payload.BlockHash, askedHash)
}

func TestNewPayloadV6ValidatedSideBlockWithUncheckedInclusionList(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	payload := bogotaPayload(t, parent)
	inclusionList := signedInclusionList(t, cfg)

	for _, tc := range []struct {
		name         string
		cached       bool
		wantInserted int
	}{
		{"unchecked inclusion list re-executes the block", false, 1},
		{"cached result answers from the quick path", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			satisfied := false
			validated := false
			var inserted []*types.Block
			stub := &stubExecutionModule{
				getHeaderFunc: func(_ context.Context, blockHash *common.Hash, _ *uint64) (*types.Header, error) {
					if blockHash != nil && (*blockHash == parent.Hash() || *blockHash == payload.BlockHash) {
						return parent, nil
					}
					return nil, nil
				},
				currentHeaderFunc: func(context.Context) (*types.Header, error) {
					return parent, nil
				},
				isValidatedFunc: func(common.Hash) bool {
					return true
				},
				insertBlocksFunc: func(_ context.Context, blocks []*types.Block) (execmodule.ExecutionStatus, error) {
					inserted = append(inserted, blocks...)
					return execmodule.ExecutionStatusSuccess, nil
				},
				validateChainFunc: func(context.Context, common.Hash, uint64) (execmodule.ValidationResult, error) {
					validated = true
					return execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusSuccess, LatestValidHash: payload.BlockHash}, nil
				},
				ilSatisfiedFunc: func(context.Context, common.Hash, uint64, types.Transactions) (*bool, error) {
					if !tc.cached && !validated {
						return nil, nil
					}
					return &satisfied, nil
				},
			}
			downloader := engine_block_downloader.NewEngineBlockDownloader(context.Background(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
			srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, ilTestMaxReorgDepth)
			srv.test = true

			status, err := srv.NewPayloadV6(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, inclusionList)
			require.NoError(t, err)
			require.NotNil(t, status)
			require.Equal(t, engine_types.ValidStatus, status.Status)
			require.NotNil(t, status.InclusionListSatisfied)
			require.False(t, *status.InclusionListSatisfied)
			require.Len(t, inserted, tc.wantInserted)
			if tc.wantInserted > 0 {
				require.True(t, validated)
				require.Len(t, inserted[0].InclusionList(), len(inclusionList))
			}
		})
	}
}

func TestNewPayloadV6InclusionListCheckErrorIsReturned(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	stub := &stubExecutionModule{
		getHeaderFunc: getHeaderReturning(parent.Hash(), parent),
		currentHeaderFunc: func(context.Context) (*types.Header, error) {
			return parent, nil
		},
		validateChainFunc: func(context.Context, common.Hash, uint64) (execmodule.ValidationResult, error) {
			return execmodule.ValidationResult{ValidationStatus: execmodule.ExecutionStatusSuccess, LatestValidHash: common.Hash{0x1}}, nil
		},
		ilSatisfiedFunc: func(context.Context, common.Hash, uint64, types.Transactions) (*bool, error) {
			return nil, errors.New("state read failed")
		},
	}
	downloader := engine_block_downloader.NewEngineBlockDownloader(context.Background(), log.New(), stub, nil, nil, cfg, ethconfig.Sync{}, nil)
	srv := NewEngineServer(log.New(), cfg, stub, downloader, false, false, false, true, nil, nil, 0, ilTestMaxReorgDepth)
	srv.test = true

	status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
	require.ErrorContains(t, err, "state read failed")
	require.Nil(t, status)
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
				ValidationStatus: execmodule.ExecutionStatusBadBlock,
				LatestValidHash:  parent.Hash(),
				ValidationError:  "bad block",
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

			srv, _ := newInclusionListServer(cfg, tc.parent, tc.validation, &satisfied, tc.maxReorgDepth)

			status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
			require.NoError(t, err)
			require.NotNil(t, status)
			require.Equal(t, tc.want, status.Status)
			require.Nil(t, status.InclusionListSatisfied)
		})
	}
}

func TestNewPayloadV6InvalidBlockHash(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{}, nil, ilTestMaxReorgDepth)
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
	}, nil, ilTestMaxReorgDepth)
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

func TestNewPayloadV6SkipsUndecodableInclusionListEntries(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, inserted := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
	}, nil, ilTestMaxReorgDepth)

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	valid := makeSignedRawTx(t, key, cfg, 0, 101, 1012)

	typed, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: 1, GasLimit: 21_000, To: &common.Address{0x22}},
		ChainID:  *cfg.ChainID,
		FeeCap:   *uint256.NewInt(1_000_000_000),
	}, *types.MakeSigner(cfg, 101, 1012), key)
	require.NoError(t, err)
	var typedBuf bytes.Buffer
	require.NoError(t, typed.MarshalBinary(&typedBuf))
	rlpWrapped, err := rlp.EncodeToBytes(typedBuf.Bytes())
	require.NoError(t, err)
	require.True(t, types.TypedTransactionMarshalledAsRlpString(rlpWrapped))

	undecodable := hexutil.Bytes{types.DynamicFeeTxType, 0xc0}
	inclusionList := []hexutil.Bytes{undecodable, rlpWrapped, valid}

	_, err = srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, inclusionList)
	require.NoError(t, err)
	require.Len(t, *inserted, 1)
	il := (*inserted)[0].InclusionList()
	require.Len(t, il, 1)
	want, err := types.DecodeTransactions([][]byte{valid})
	require.NoError(t, err)
	require.Equal(t, want[0].Hash(), il[0].Hash())
}

func TestNewPayloadV6AcceptsEmptyInclusionList(t *testing.T) {
	t.Parallel()

	satisfied := true
	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
	}, &satisfied, ilTestMaxReorgDepth)

	status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, []hexutil.Bytes{})
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, engine_types.ValidStatus, status.Status)
}

func TestPayloadStatusMarshalsInclusionListSatisfied(t *testing.T) {
	t.Parallel()

	satisfied := false
	for _, tc := range []struct {
		name   string
		status engine_types.PayloadStatus
		want   string
	}{
		{"nil omitted", engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, ""},
		{"V6 nil is null", engine_types.PayloadStatus{Status: engine_types.SyncingStatus, SSZVersion: clparams.HezeVersion}, `null`},
		{"false", engine_types.PayloadStatus{Status: engine_types.ValidStatus, InclusionListSatisfied: &satisfied}, `false`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(tc.status)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			if tc.want == "" {
				require.NotContains(t, fields, "inclusionListSatisfied")
				return
			}
			require.Contains(t, fields, "inclusionListSatisfied")
			require.JSONEq(t, tc.want, string(fields["inclusionListSatisfied"]))
		})
	}
}

func TestNewPayloadV1ToV5LeaveInclusionListSatisfiedNil(t *testing.T) {
	t.Parallel()

	satisfied := true
	for _, tc := range []struct {
		name    string
		cfg     *chain.Config
		version clparams.StateVersion
	}{
		{"V1", preShanghaiChainConfig(), clparams.BellatrixVersion},
		{"V2", preCancunChainConfig(), clparams.CapellaVersion},
		{"V3", prePragueChainConfig(), clparams.DenebVersion},
		{"V4", preOsakaChainConfig(), clparams.ElectraVersion},
		{"V5", allForksChainConfig(), clparams.GloasVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parent := makeParentHeader(1000)
			srv, _ := newInclusionListServer(tc.cfg, parent, execmodule.ValidationResult{
				ValidationStatus: execmodule.ExecutionStatusSuccess,
				LatestValidHash:  common.Hash{0x1},
			}, &satisfied, ilTestMaxReorgDepth)
			payload := preBogotaPayload(t, parent, tc.version)

			var status *engine_types.PayloadStatus
			var err error
			switch tc.version {
			case clparams.BellatrixVersion:
				status, err = srv.NewPayloadV1(t.Context(), payload)
			case clparams.CapellaVersion:
				status, err = srv.NewPayloadV2(t.Context(), payload)
			case clparams.DenebVersion:
				status, err = srv.NewPayloadV3(t.Context(), payload, []common.Hash{}, &common.Hash{})
			case clparams.ElectraVersion:
				status, err = srv.NewPayloadV4(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{})
			case clparams.GloasVersion:
				status, err = srv.NewPayloadV5(t.Context(), payload, []common.Hash{}, &common.Hash{}, []hexutil.Bytes{})
			}
			require.NoError(t, err)
			require.NotNil(t, status)
			require.Equal(t, engine_types.ValidStatus, status.Status)
			require.Nil(t, status.InclusionListSatisfied)
			require.True(t, status.SSZVersion.Before(clparams.HezeVersion))

			raw, err := json.Marshal(status)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			require.NotContains(t, fields, "inclusionListSatisfied")
		})
	}
}

func TestNewPayloadV6TagsHezeSSZVersion(t *testing.T) {
	t.Parallel()

	satisfied := true
	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
		LatestValidHash:  common.Hash{0x1},
	}, &satisfied, ilTestMaxReorgDepth)

	status, err := srv.NewPayloadV6(t.Context(), bogotaPayload(t, parent), []common.Hash{}, &common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg))
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, clparams.HezeVersion, status.SSZVersion)

	enc, err := status.EncodeSSZ(nil)
	require.NoError(t, err)
	var out engine_types.PayloadStatus
	require.NoError(t, out.DecodeSSZ(enc, int(clparams.HezeVersion)))
	require.NotNil(t, out.InclusionListSatisfied)
	require.True(t, *out.InclusionListSatisfied)
}

func TestNewPayloadV6JSONRPC(t *testing.T) {
	t.Parallel()

	satisfied := true
	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
	}, &satisfied, ilTestMaxReorgDepth)
	client := newEngineInProcClient(t, srv)

	var status engine_types.PayloadStatus
	require.NoError(t, client.CallContext(t.Context(), &status, "engine_newPayloadV6",
		bogotaPayload(t, parent), []common.Hash{}, common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg)))
	require.Equal(t, engine_types.ValidStatus, status.Status)
	require.NotNil(t, status.InclusionListSatisfied)
	require.True(t, *status.InclusionListSatisfied)
}

func TestNewPayloadV6JSONRPCSyncingHasNullInclusionListSatisfied(t *testing.T) {
	t.Parallel()

	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, _ := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
		LatestValidHash:  common.Hash{0x1},
	}, nil, ilTestMaxReorgDepth)
	client := newEngineInProcClient(t, srv)

	var fields map[string]json.RawMessage
	require.NoError(t, client.CallContext(t.Context(), &fields, "engine_newPayloadV6",
		bogotaPayload(t, parent), []common.Hash{}, common.Hash{}, []hexutil.Bytes{}, signedInclusionList(t, cfg)))
	require.JSONEq(t, `"SYNCING"`, string(fields["status"]))
	require.Contains(t, fields, "inclusionListSatisfied")
	require.JSONEq(t, `null`, string(fields["inclusionListSatisfied"]))
}

func TestNewPayloadV6JSONRPCClient(t *testing.T) {
	t.Parallel()

	satisfied := true
	cfg := bogotaChainConfig()
	parent := makeParentHeader(1000)
	srv, inserted := newInclusionListServer(cfg, parent, execmodule.ValidationResult{
		ValidationStatus: execmodule.ExecutionStatusSuccess,
	}, &satisfied, ilTestMaxReorgDepth)
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
