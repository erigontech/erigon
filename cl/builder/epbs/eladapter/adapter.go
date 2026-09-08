package eladapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

var (
	ErrExecutionBusy  = errors.New("execution module busy")
	ErrUnknownPayload = errors.New("unknown payload")
	ErrInvalidResult  = errors.New("invalid assembled payload")
)

// Adapter converts the in-process execution builder result into consensus-layer payload types.
type Adapter struct {
	execution execmodule.ExecutionModule
	version   clparams.StateVersion
	beaconCfg *clparams.BeaconChainConfig
}

// NewAdapter creates an execution-layer adapter for the active consensus version.
func NewAdapter(execution execmodule.ExecutionModule, version clparams.StateVersion, beaconCfg *clparams.BeaconChainConfig) *Adapter {
	return &Adapter{execution: execution, version: version, beaconCfg: beaconCfg}
}

func (a *Adapter) AssemblePayload(ctx context.Context, parameters *builder.Parameters) (uint64, error) {
	if a == nil || a.execution == nil {
		return 0, fmt.Errorf("eladapter: nil execution module")
	}
	if parameters == nil {
		return 0, fmt.Errorf("eladapter: nil build parameters")
	}
	result, err := a.execution.AssembleBlock(ctx, parameters)
	if err != nil {
		return 0, fmt.Errorf("eladapter: assemble block: %w", err)
	}
	if result.Busy {
		return 0, ErrExecutionBusy
	}
	return result.PayloadID, nil
}

func (a *Adapter) GetPayload(ctx context.Context, payloadID uint64) (*AssembledPayload, error) {
	if a == nil || a.execution == nil {
		return nil, fmt.Errorf("eladapter: nil execution module")
	}
	if a.beaconCfg == nil {
		return nil, fmt.Errorf("eladapter: nil beacon config")
	}
	result, err := a.execution.GetAssembledBlock(ctx, payloadID)
	if err != nil {
		return nil, fmt.Errorf("eladapter: get assembled block %d: %w", payloadID, err)
	}
	if result.Unknown {
		return nil, fmt.Errorf("eladapter: payload %d: %w", payloadID, ErrUnknownPayload)
	}
	if result.Busy || result.Block == nil {
		return nil, nil
	}
	return a.convertResult(&result)
}

func (a *Adapter) convertResult(result *execmodule.AssembledBlockResult) (*AssembledPayload, error) {
	if result.Block.Block == nil {
		return nil, fmt.Errorf("%w: nil block", ErrInvalidResult)
	}
	if result.BlockValue == nil {
		return nil, fmt.Errorf("%w: nil block value", ErrInvalidResult)
	}
	block := result.Block.Block
	if block.HeaderNoCopy() == nil {
		return nil, fmt.Errorf("%w: nil block header", ErrInvalidResult)
	}
	header := block.Header()

	encodedTransactions, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, fmt.Errorf("eladapter: marshal transactions: %w", err)
	}

	payload := cltypes.NewEth1Block(a.version, a.beaconCfg)
	payload.ParentHash = header.ParentHash
	payload.FeeRecipient = header.Coinbase
	payload.StateRoot = header.Root
	payload.ReceiptsRoot = header.ReceiptHash
	payload.LogsBloom = header.Bloom
	payload.PrevRandao = header.MixDigest
	payload.BlockNumber = header.Number.Uint64()
	payload.GasLimit = header.GasLimit
	payload.GasUsed = header.GasUsed
	payload.Time = header.Time
	payload.Extra = solid.NewExtraData()
	payload.Extra.SetBytes(header.Extra)
	if header.BaseFee != nil {
		_, _ = header.BaseFee.MarshalSSZAppend(payload.BaseFeePerGas[:0])
	}
	payload.BlockHash = block.Hash()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(encodedTransactions)

	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(a.beaconCfg.MaxWithdrawalsPerPayload), 44)
	for _, withdrawal := range block.Withdrawals() {
		payload.Withdrawals.Append(&cltypes.Withdrawal{
			Amount:    withdrawal.Amount,
			Address:   withdrawal.Address,
			Index:     withdrawal.Index,
			Validator: withdrawal.Validator,
		})
	}
	if header.ExcessBlobGas != nil {
		payload.ExcessBlobGas = *header.ExcessBlobGas
	}
	if header.BlobGasUsed != nil {
		payload.BlobGasUsed = *header.BlobGasUsed
	}

	if a.version >= clparams.GloasVersion {
		if header.SlotNumber == nil {
			return nil, fmt.Errorf("%w: nil slot number", ErrInvalidResult)
		}
		if header.BlockAccessListHash == nil {
			return nil, fmt.Errorf("%w: nil block access list hash", ErrInvalidResult)
		}
		payload.SlotNumber = *header.SlotNumber
		if err := setBlockAccessList(payload, block.BlockAccessListSidecar(), *header.BlockAccessListHash); err != nil {
			return nil, err
		}
	}

	engineBundle, err := engine_types.BlobsBundleFromTransactions(block.Transactions())
	if err != nil {
		return nil, fmt.Errorf("eladapter: blob bundle: %w", err)
	}
	blobsBundle := convertBlobsBundle(engineBundle)

	var requestsBundle *typesproto.RequestsBundle
	if result.Block.Requests != nil {
		requestsBundle = &typesproto.RequestsBundle{}
		for _, request := range result.Block.Requests {
			requestsBundle.Requests = append(requestsBundle.Requests, request.Encode())
		}
	}

	return &AssembledPayload{
		Eth1Block:      payload,
		BlobsBundle:    blobsBundle,
		RequestsBundle: requestsBundle,
		BlockValue:     result.BlockValue.ToBig(),
	}, nil
}

func setBlockAccessList(payload *cltypes.Eth1Block, sidecar *types.BlockAccessListSidecar, expectedHash common.Hash) error {
	if sidecar == nil {
		if expectedHash != empty.BlockAccessListHash {
			return fmt.Errorf("%w: block access list hash mismatch", ErrInvalidResult)
		}
		return nil
	}
	actualHash, err := sidecar.Hash()
	if err != nil {
		return fmt.Errorf("eladapter: hash block access list: %w", err)
	}
	if actualHash != expectedHash {
		return fmt.Errorf("%w: block access list hash mismatch", ErrInvalidResult)
	}
	encoded, err := sidecar.Bytes()
	if err != nil {
		return fmt.Errorf("eladapter: encode block access list: %w", err)
	}
	if err := payload.BlockAccessList.SetBytes(encoded); err != nil {
		return fmt.Errorf("eladapter: set block access list: %w", err)
	}
	return nil
}

func convertBlobsBundle(bundle *engine_types.BlobsBundle) *BlobsBundle {
	if bundle == nil {
		return nil
	}
	commitments := make([][]byte, len(bundle.Commitments))
	for i, commitment := range bundle.Commitments {
		commitments[i] = []byte(commitment)
	}
	proofs := make([][]byte, len(bundle.Proofs))
	for i, proof := range bundle.Proofs {
		proofs[i] = []byte(proof)
	}
	blobs := make([][]byte, len(bundle.Blobs))
	for i, blob := range bundle.Blobs {
		blobs[i] = []byte(blob)
	}
	return &BlobsBundle{Commitments: commitments, Proofs: proofs, Blobs: blobs}
}
