package eladapter

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type assembledBlockModule struct {
	execmodule.ExecutionModule
	assembleResult execmodule.AssembleBlockResult
	assembleErr    error
	assembled      execmodule.AssembledBlockResult
	assembledErr   error
}

func (m assembledBlockModule) AssembleBlock(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	return m.assembleResult, m.assembleErr
}

func (m assembledBlockModule) GetAssembledBlock(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
	return m.assembled, m.assembledErr
}

func TestAdapterStartsAvailableBuild(t *testing.T) {
	adapter := NewAdapter(assembledBlockModule{assembleResult: execmodule.AssembleBlockResult{PayloadID: 42}}, &clparams.MainnetBeaconConfig)
	payloadID, err := adapter.AssemblePayload(t.Context(), &builder.Parameters{})
	require.NoError(t, err)
	require.Equal(t, uint64(42), payloadID)
}

func TestAdapterRejectsBusyBuild(t *testing.T) {
	adapter := NewAdapter(assembledBlockModule{assembleResult: execmodule.AssembleBlockResult{Busy: true}}, &clparams.MainnetBeaconConfig)
	_, err := adapter.AssemblePayload(t.Context(), &builder.Parameters{})
	require.ErrorIs(t, err, ErrExecutionBusy)
}

func TestAdapterPreservesBlockAccessList(t *testing.T) {
	accessList := types.BlockAccessList{{Address: accounts.InternAddress(common.Address{19: 1})}}
	accessListSidecar := types.NewBlockAccessListSidecar(accessList)
	accessListHash, err := accessListSidecar.Hash()
	require.NoError(t, err)
	parentRoot := common.Hash{31: 1}
	requests := types.FlatRequests{}
	requestsHash := *requests.Hash()
	slot := uint64(10)
	zero := uint64(0)
	header := &types.Header{
		Number:                *uint256.NewInt(1),
		BaseFee:               uint256.NewInt(1),
		GasLimit:              30_000_000,
		Time:                  1,
		ParentBeaconBlockRoot: &parentRoot,
		RequestsHash:          &requestsHash,
		BlockAccessListHash:   &accessListHash,
		SlotNumber:            &slot,
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
	}
	block := types.NewBlock(header, nil, nil, nil, []*types.Withdrawal{}, accessListSidecar)
	module := assembledBlockModule{assembled: execmodule.AssembledBlockResult{
		Block:      &types.BlockWithReceipts{Block: block, Requests: requests},
		BlockValue: uint256.NewInt(1),
	}}

	payload, err := NewAdapter(module, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.NoError(t, err)
	wantAccessList, err := accessListSidecar.Bytes()
	require.NoError(t, err)
	require.Equal(t, wantAccessList, payload.Eth1Block.BlockAccessList.Bytes())
	_, err = execution_client.DecodeAndValidateBlockAccessList(payload.Eth1Block)
	require.NoError(t, err)

	decodedAccessList, err := types.DecodeBlockAccessListSidecarOwned(bytes.Clone(payload.Eth1Block.BlockAccessList.Bytes()))
	require.NoError(t, err)
	rebuiltHeader, err := payload.Eth1Block.RlpHeader(&parentRoot, requestsHash, decodedAccessList)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), rebuiltHeader.Hash())
}

func TestAdapterPayloadRoundTripsStrictSSZ(t *testing.T) {
	payload, err := NewAdapter(assembledBlockModule{assembled: validAssembledResult()}, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.NoError(t, err)
	require.NotNil(t, payload.RequestsBundle)

	encoded, err := payload.Eth1Block.EncodeSSZ(nil)
	require.NoError(t, err)
	decoded := cltypes.NewEth1Block(clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	require.NoError(t, decoded.DecodeSSZStrict(encoded, int(clparams.GloasVersion)))
	require.Equal(t, payload.Eth1Block.BlockHash, decoded.BlockHash)
	_, err = execution_client.DecodeAndValidateBlockAccessList(decoded)
	require.NoError(t, err)
}

func TestAdapterRejectsBlockAccessListOverGasLimit(t *testing.T) {
	result := validAssembledResult()
	accessList := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: accounts.InternAddress(common.Address{19: 1})}})
	accessListHash, err := accessList.Hash()
	require.NoError(t, err)
	header := result.Block.Block.Header()
	header.GasLimit = 0
	header.BlockAccessListHash = &accessListHash
	result.Block.Block = types.NewBlock(header, result.Block.Block.Transactions(), nil, nil, result.Block.Block.Withdrawals(), accessList)

	_, err = NewAdapter(assembledBlockModule{assembled: result}, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, ErrInvalidResult)
}

func TestAdapterClassifiesUnavailablePayloads(t *testing.T) {
	payload, err := NewAdapter(assembledBlockModule{assembled: execmodule.AssembledBlockResult{Busy: true}}, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.NoError(t, err)
	require.Nil(t, payload)

	_, err = NewAdapter(assembledBlockModule{}, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, ErrInvalidResult)

	_, err = NewAdapter(assembledBlockModule{assembled: execmodule.AssembledBlockResult{Unknown: true}}, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, ErrUnknownPayload)
}

func TestAdapterRejectsMalformedExecutionResult(t *testing.T) {
	header := &types.Header{Number: *uint256.NewInt(1)}
	block := types.NewBlock(header, nil, nil, nil, nil, nil)
	module := assembledBlockModule{assembled: execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{Block: block}}}
	_, err := NewAdapter(module, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "nil block value")

	module.assembled = execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{}, BlockValue: uint256.NewInt(1)}
	_, err = NewAdapter(module, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "nil block")
}

func TestAdapterRejectsInvalidGloasFields(t *testing.T) {
	result := validAssembledResult()
	result.Block.Block.HeaderNoCopy().SlotNumber = nil
	module := assembledBlockModule{assembled: result}
	_, err := NewAdapter(module, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "nil slot number")

	result = validAssembledResult()
	accessList := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: accounts.InternAddress(common.Address{19: 1})}})
	block := result.Block.Block
	module.assembled = result
	module.assembled.Block.Block = types.NewBlock(block.Header(), block.Transactions(), nil, nil, block.Withdrawals(), accessList)
	_, err = NewAdapter(module, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "block access list hash mismatch")
}

func TestAdapterRejectsInvalidPayloadBoundaries(t *testing.T) {
	for name, mutate := range map[string]func(*execmodule.AssembledBlockResult){
		"number overflow": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().Number.SetBytes([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0})
		},
		"extra data too long": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().Extra = make([]byte, clparams.MainnetBeaconConfig.MaxExtraDataBytes+1)
		},
		"nil base fee": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().BaseFee = nil
		},
		"nil withdrawals root": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().WithdrawalsHash = nil
		},
		"nil blob gas used": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().BlobGasUsed = nil
		},
		"nil excess blob gas": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().ExcessBlobGas = nil
		},
		"nil parent beacon block root": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().ParentBeaconBlockRoot = nil
		},
		"nil requests hash": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().RequestsHash = nil
		},
		"nil requests": func(result *execmodule.AssembledBlockResult) {
			result.Block.Requests = nil
		},
		"requests hash mismatch": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().RequestsHash = new(common.Hash)
		},
		"transactions root mismatch": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().TxHash = common.Hash{}
		},
		"withdrawals root mismatch": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.HeaderNoCopy().WithdrawalsHash = new(common.Hash)
		},
		"nil transaction": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.Transactions()[0] = nil
		},
		"nil withdrawal": func(result *execmodule.AssembledBlockResult) {
			result.Block.Block.Withdrawals()[0] = nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			result := validAssembledResult()
			mutate(&result)
			require.NotPanics(t, func() {
				_, err := NewAdapter(assembledBlockModule{assembled: result}, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
				require.ErrorIs(t, err, ErrInvalidResult)
			})
		})
	}
}

func TestAdapterRejectsExtraDataBeyondRepresentationLimit(t *testing.T) {
	config := clparams.MainnetBeaconConfig
	config.MaxExtraDataBytes = 33
	result := validAssembledResult()
	result.Block.Block.HeaderNoCopy().Extra = make([]byte, 33)

	_, err := NewAdapter(assembledBlockModule{assembled: result}, &config).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, ErrInvalidResult)
}

func TestAdapterRejectsMalformedExecutionRequests(t *testing.T) {
	for name, requests := range map[string]types.FlatRequests{
		"empty request data":   {{Type: types.DepositRequestType}},
		"unknown request type": {{Type: 0xff, RequestData: []byte{1}}},
		"duplicate request type": {
			{Type: types.DepositRequestType, RequestData: []byte{1}},
			{Type: types.DepositRequestType, RequestData: []byte{1}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			result := validAssembledResult()
			result.Block.Requests = requests
			result.Block.Block.HeaderNoCopy().RequestsHash = requests.Hash()

			_, err := NewAdapter(assembledBlockModule{assembled: result}, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
			require.ErrorIs(t, err, ErrInvalidResult)
		})
	}
}

func TestAdapterEnforcesConfiguredPayloadLimits(t *testing.T) {
	config := clparams.MainnetBeaconConfig
	config.MaxTransactionsPerPayload = 0
	_, err := NewAdapter(assembledBlockModule{assembled: validAssembledResult()}, &config).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, ErrInvalidResult)

	config = clparams.MainnetBeaconConfig
	config.MaxBytesPerTransaction = 1
	_, err = NewAdapter(assembledBlockModule{assembled: validAssembledResult()}, &config).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, ErrInvalidResult)

	config = clparams.MainnetBeaconConfig
	config.MaxWithdrawalsPerPayload = 0
	_, err = NewAdapter(assembledBlockModule{assembled: validAssembledResult()}, &config).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, ErrInvalidResult)
}

func TestAdapterPropagatesExecutionErrors(t *testing.T) {
	want := errors.New("execution failed")
	_, err := NewAdapter(assembledBlockModule{assembleErr: want}, &clparams.MainnetBeaconConfig).AssemblePayload(t.Context(), &builder.Parameters{})
	require.ErrorIs(t, err, want)

	_, err = NewAdapter(assembledBlockModule{assembledErr: want}, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, want)
}

func TestAdapterRejectsMissingDependencies(t *testing.T) {
	_, err := NewAdapter(nil, &clparams.MainnetBeaconConfig).AssemblePayload(t.Context(), &builder.Parameters{})
	require.ErrorContains(t, err, "nil execution module")

	module := assembledBlockModule{}
	_, err = NewAdapter(module, &clparams.MainnetBeaconConfig).AssemblePayload(t.Context(), nil)
	require.ErrorContains(t, err, "nil build parameters")

	_, err = NewAdapter(module, nil).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "nil beacon config")
}

func validAssembledResult() execmodule.AssembledBlockResult {
	zero := uint64(0)
	slot := uint64(1)
	parentRoot := common.Hash{31: 1}
	requests := types.FlatRequests{}
	requestsHash := requests.Hash()
	blockAccessListHash := empty.BlockAccessListHash
	header := &types.Header{
		Number:                *uint256.NewInt(1),
		BaseFee:               uint256.NewInt(1),
		GasLimit:              30_000_000,
		Time:                  1,
		ParentBeaconBlockRoot: &parentRoot,
		RequestsHash:          requestsHash,
		BlockAccessListHash:   &blockAccessListHash,
		SlotNumber:            &slot,
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
	}
	tx := types.NewTransaction(0, common.Address{}, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil)
	block := types.NewBlock(header, []types.Transaction{tx}, nil, nil, []*types.Withdrawal{{}}, nil)
	return execmodule.AssembledBlockResult{
		Block:      &types.BlockWithReceipts{Block: block, Requests: requests},
		BlockValue: uint256.NewInt(1),
	}
}
