package eladapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type assembledBlockModule struct {
	execmodule.ExecutionModule
	assembleResult execmodule.AssembleBlockResult
	assembled      execmodule.AssembledBlockResult
}

func (m assembledBlockModule) AssembleBlock(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error) {
	return m.assembleResult, nil
}

func (m assembledBlockModule) GetAssembledBlock(context.Context, uint64) (execmodule.AssembledBlockResult, error) {
	return m.assembled, nil
}

func TestAdapterStartsAvailableBuild(t *testing.T) {
	adapter := NewAdapter(assembledBlockModule{assembleResult: execmodule.AssembleBlockResult{PayloadID: 42}}, clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	payloadID, err := adapter.AssemblePayload(t.Context(), &builder.Parameters{})
	require.NoError(t, err)
	require.Equal(t, uint64(42), payloadID)
}

func TestAdapterRejectsBusyBuild(t *testing.T) {
	adapter := NewAdapter(assembledBlockModule{assembleResult: execmodule.AssembleBlockResult{Busy: true}}, clparams.GloasVersion, &clparams.MainnetBeaconConfig)
	_, err := adapter.AssemblePayload(t.Context(), &builder.Parameters{})
	require.ErrorIs(t, err, ErrExecutionBusy)
}

func TestAdapterPreservesBlockAccessList(t *testing.T) {
	accessList := types.BlockAccessList{{Address: accounts.InternAddress(common.Address{19: 1})}}
	accessListSidecar := types.NewBlockAccessListSidecar(accessList)
	accessListHash, err := accessListSidecar.Hash()
	require.NoError(t, err)
	parentRoot := common.Hash{31: 1}
	requestsHash := common.Hash{31: 2}
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
		Block:      &types.BlockWithReceipts{Block: block},
		BlockValue: uint256.NewInt(1),
	}}

	payload, err := NewAdapter(module, clparams.GloasVersion, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.NoError(t, err)
	wantAccessList, err := accessListSidecar.Bytes()
	require.NoError(t, err)
	require.Equal(t, wantAccessList, payload.Eth1Block.BlockAccessList.Bytes())

	decodedAccessList, err := types.DecodeBlockAccessListSidecarOwned(bytes.Clone(payload.Eth1Block.BlockAccessList.Bytes()))
	require.NoError(t, err)
	rebuiltHeader, err := payload.Eth1Block.RlpHeader(&parentRoot, requestsHash, decodedAccessList)
	require.NoError(t, err)
	require.Equal(t, block.Hash(), rebuiltHeader.Hash())
}

func TestAdapterClassifiesUnavailablePayloads(t *testing.T) {
	for name, result := range map[string]execmodule.AssembledBlockResult{
		"busy":    {Busy: true},
		"pending": {},
	} {
		t.Run(name, func(t *testing.T) {
			payload, err := NewAdapter(assembledBlockModule{assembled: result}, clparams.GloasVersion, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
			require.NoError(t, err)
			require.Nil(t, payload)
		})
	}

	_, err := NewAdapter(assembledBlockModule{assembled: execmodule.AssembledBlockResult{Unknown: true}}, clparams.GloasVersion, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorIs(t, err, ErrUnknownPayload)
}

func TestAdapterRejectsMalformedExecutionResult(t *testing.T) {
	header := &types.Header{Number: *uint256.NewInt(1)}
	block := types.NewBlock(header, nil, nil, nil, nil, nil)
	module := assembledBlockModule{assembled: execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{Block: block}}}
	_, err := NewAdapter(module, clparams.GloasVersion, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "nil block value")

	module.assembled = execmodule.AssembledBlockResult{Block: &types.BlockWithReceipts{}, BlockValue: uint256.NewInt(1)}
	_, err = NewAdapter(module, clparams.GloasVersion, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "nil block")
}

func TestAdapterRejectsInvalidGloasFields(t *testing.T) {
	zero := uint64(0)
	header := &types.Header{
		Number:              *uint256.NewInt(1),
		BaseFee:             uint256.NewInt(1),
		BlobGasUsed:         &zero,
		ExcessBlobGas:       &zero,
		BlockAccessListHash: new(common.Hash),
	}
	block := types.NewBlock(header, nil, nil, nil, []*types.Withdrawal{}, nil)
	module := assembledBlockModule{assembled: execmodule.AssembledBlockResult{
		Block:      &types.BlockWithReceipts{Block: block},
		BlockValue: uint256.NewInt(1),
	}}
	_, err := NewAdapter(module, clparams.GloasVersion, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "nil slot number")

	slot := uint64(1)
	header.SlotNumber = &slot
	accessList := types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: accounts.InternAddress(common.Address{19: 1})}})
	module.assembled.Block = &types.BlockWithReceipts{Block: types.NewBlock(header, nil, nil, nil, []*types.Withdrawal{}, accessList)}
	_, err = NewAdapter(module, clparams.GloasVersion, &clparams.MainnetBeaconConfig).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "block access list hash mismatch")
}

func TestAdapterRejectsMissingDependencies(t *testing.T) {
	_, err := NewAdapter(nil, clparams.GloasVersion, &clparams.MainnetBeaconConfig).AssemblePayload(t.Context(), &builder.Parameters{})
	require.ErrorContains(t, err, "nil execution module")

	module := assembledBlockModule{}
	_, err = NewAdapter(module, clparams.GloasVersion, &clparams.MainnetBeaconConfig).AssemblePayload(t.Context(), nil)
	require.ErrorContains(t, err, "nil build parameters")

	_, err = NewAdapter(module, clparams.GloasVersion, nil).GetPayload(t.Context(), 1)
	require.ErrorContains(t, err, "nil beacon config")
}
