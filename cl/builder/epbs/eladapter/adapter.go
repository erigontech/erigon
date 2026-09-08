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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/params"
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
	beaconCfg *clparams.BeaconChainConfig
}

func NewAdapter(execution execmodule.ExecutionModule, beaconCfg *clparams.BeaconChainConfig) *Adapter {
	return &Adapter{execution: execution, beaconCfg: beaconCfg}
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
	if result.Busy {
		return nil, nil
	}
	if result.Block == nil {
		return nil, fmt.Errorf("%w: builder produced no block", ErrInvalidResult)
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
	if !header.Number.IsUint64() {
		return nil, fmt.Errorf("%w: block number overflows uint64", ErrInvalidResult)
	}
	extraDataLimit := min(a.beaconCfg.MaxExtraDataBytes, params.MaximumExtraDataSize)
	if uint64(len(header.Extra)) > extraDataLimit {
		return nil, fmt.Errorf("%w: extra data length %d exceeds limit %d", ErrInvalidResult, len(header.Extra), extraDataLimit)
	}
	if header.BaseFee == nil {
		return nil, fmt.Errorf("%w: nil base fee", ErrInvalidResult)
	}
	if header.WithdrawalsHash == nil {
		return nil, fmt.Errorf("%w: nil withdrawals root", ErrInvalidResult)
	}
	if header.BlobGasUsed == nil {
		return nil, fmt.Errorf("%w: nil blob gas used", ErrInvalidResult)
	}
	if header.ExcessBlobGas == nil {
		return nil, fmt.Errorf("%w: nil excess blob gas", ErrInvalidResult)
	}
	if header.ParentBeaconBlockRoot == nil {
		return nil, fmt.Errorf("%w: nil parent beacon block root", ErrInvalidResult)
	}
	if header.RequestsHash == nil {
		return nil, fmt.Errorf("%w: nil requests hash", ErrInvalidResult)
	}
	if result.Block.Requests == nil {
		return nil, fmt.Errorf("%w: nil execution requests", ErrInvalidResult)
	}
	if requestsHash := result.Block.Requests.Hash(); requestsHash == nil || *requestsHash != *header.RequestsHash {
		return nil, fmt.Errorf("%w: execution requests hash mismatch", ErrInvalidResult)
	}
	encodedRequests := make([]hexutil.Bytes, len(result.Block.Requests))
	for i := range result.Block.Requests {
		encodedRequests[i] = result.Block.Requests[i].Encode()
	}
	if _, err := cltypes.DecodeExecutionRequestsList(a.beaconCfg, encodedRequests, clparams.GloasVersion); err != nil {
		return nil, fmt.Errorf("%w: decode execution requests: %w", ErrInvalidResult, err)
	}

	transactions := block.Transactions()
	if uint64(len(transactions)) > a.beaconCfg.MaxTransactionsPerPayload {
		return nil, fmt.Errorf("%w: transaction count %d exceeds limit %d", ErrInvalidResult, len(transactions), a.beaconCfg.MaxTransactionsPerPayload)
	}
	for i, transaction := range transactions {
		if transaction == nil {
			return nil, fmt.Errorf("%w: nil transaction at index %d", ErrInvalidResult, i)
		}
	}

	encodedTransactions, err := types.MarshalTransactionsBinary(transactions)
	if err != nil {
		return nil, fmt.Errorf("eladapter: marshal transactions: %w", err)
	}
	for i, transaction := range encodedTransactions {
		if uint64(len(transaction)) > a.beaconCfg.MaxBytesPerTransaction {
			return nil, fmt.Errorf("%w: transaction %d length %d exceeds limit %d", ErrInvalidResult, i, len(transaction), a.beaconCfg.MaxBytesPerTransaction)
		}
	}

	withdrawals := block.Withdrawals()
	if uint64(len(withdrawals)) > a.beaconCfg.MaxWithdrawalsPerPayload {
		return nil, fmt.Errorf("%w: withdrawal count %d exceeds limit %d", ErrInvalidResult, len(withdrawals), a.beaconCfg.MaxWithdrawalsPerPayload)
	}
	for i, withdrawal := range withdrawals {
		if withdrawal == nil {
			return nil, fmt.Errorf("%w: nil withdrawal at index %d", ErrInvalidResult, i)
		}
	}

	payload := cltypes.NewEth1Block(clparams.GloasVersion, a.beaconCfg)
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
	if _, err := header.BaseFee.MarshalSSZAppend(payload.BaseFeePerGas[:0]); err != nil {
		return nil, fmt.Errorf("eladapter: encode base fee: %w", err)
	}
	payload.BlockHash = block.Hash()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(encodedTransactions)

	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(a.beaconCfg.MaxWithdrawalsPerPayload), 44)
	for _, withdrawal := range withdrawals {
		payload.Withdrawals.Append(&cltypes.Withdrawal{
			Amount:    withdrawal.Amount,
			Address:   withdrawal.Address,
			Index:     withdrawal.Index,
			Validator: withdrawal.Validator,
		})
	}
	payload.ExcessBlobGas = *header.ExcessBlobGas
	payload.BlobGasUsed = *header.BlobGasUsed
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
	if _, err := payload.RlpHeader(header.ParentBeaconBlockRoot, *header.RequestsHash, block.BlockAccessListSidecar()); err != nil {
		return nil, fmt.Errorf("%w: reconstruct execution header: %w", ErrInvalidResult, err)
	}

	engineBundle, err := engine_types.BlobsBundleFromTransactions(transactions)
	if err != nil {
		return nil, fmt.Errorf("eladapter: blob bundle: %w", err)
	}
	blobsBundle := convertBlobsBundle(engineBundle)

	requestsBundle := &typesproto.RequestsBundle{Requests: make([][]byte, len(encodedRequests))}
	for i := range encodedRequests {
		requestsBundle.Requests[i] = encodedRequests[i]
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
		sidecar = types.NewBlockAccessListSidecar(types.BlockAccessList{})
	}
	actualHash, err := sidecar.Hash()
	if err != nil {
		return fmt.Errorf("eladapter: hash block access list: %w", err)
	}
	if actualHash != expectedHash {
		return fmt.Errorf("%w: block access list hash mismatch", ErrInvalidResult)
	}
	if err := sidecar.ValidateForBlock(payload.GasLimit); err != nil {
		return fmt.Errorf("%w: validate block access list: %w", ErrInvalidResult, err)
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
