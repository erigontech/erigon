package eladapter

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

// Adapter wraps an execution module to provide the PayloadAssembler interface
// expected by the ePBS builder.
type Adapter struct {
	exec      execmodule.ExecutionModule
	version   clparams.StateVersion
	beaconCfg *clparams.BeaconChainConfig
}

// NewAdapter creates an Adapter backed by the given execution module.
func NewAdapter(exec execmodule.ExecutionModule, version clparams.StateVersion, beaconCfg *clparams.BeaconChainConfig) *Adapter {
	return &Adapter{exec: exec, version: version, beaconCfg: beaconCfg}
}

// AssemblePayload initiates a new EL block build with the given parameters.
// Returns the payloadId on success, or an error if the EL is busy/failing.
func (a *Adapter) AssemblePayload(ctx context.Context, params *builder.Parameters) (uint64, error) {
	result, err := a.exec.AssembleBlock(ctx, params)
	if err != nil {
		return 0, fmt.Errorf("eladapter: AssembleBlock: %w", err)
	}
	if result.Busy {
		return 0, fmt.Errorf("eladapter: execution module busy")
	}
	return result.PayloadID, nil
}

// GetPayload retrieves the assembled block for the given payloadId and converts
// it to CL-compatible types. Returns nil (no error) if the build is not ready.
func (a *Adapter) GetPayload(ctx context.Context, payloadId uint64) (*AssembledPayload, error) {
	result, err := a.exec.GetAssembledBlock(ctx, payloadId)
	if err != nil {
		return nil, fmt.Errorf("eladapter: GetAssembledBlock(%d): %w", payloadId, err)
	}
	if result.Busy || result.Block == nil {
		return nil, nil // not ready yet
	}
	return convertResult(a.version, a.beaconCfg, &result)
}

// convertResult converts an AssembledBlockResult into CL-compatible types.
// Mirrors the logic in execmodule/chainreader/chain_reader.go:GetAssembledBlock.
func convertResult(version clparams.StateVersion, beaconCfg *clparams.BeaconChainConfig, result *execmodule.AssembledBlockResult) (*AssembledPayload, error) {
	br := result.Block
	block := br.Block
	header := block.Header()

	blockValue := result.BlockValue.ToBig()

	// Encode transactions
	encodedTxs, err := types.MarshalTransactionsBinary(block.Transactions())
	if err != nil {
		return nil, fmt.Errorf("eladapter: marshal transactions: %w", err)
	}

	// BaseFeePerGas in cltypes.Eth1Block is stored as little-endian bytes in a common.Hash.
	var baseFeeLE common.Hash
	if header.BaseFee != nil {
		be := header.BaseFee.Bytes32() // big-endian [32]byte
		copy(baseFeeLE[:], be[:])
		utils.ReverseBytes(&baseFeeLE) // convert to little-endian
	}

	extraData := solid.NewExtraData()
	extraData.SetBytes(header.Extra)

	// Build a fully-initialized CL Eth1Block
	eth1Block := cltypes.NewEth1Block(version, beaconCfg)
	eth1Block.ParentHash = header.ParentHash
	eth1Block.FeeRecipient = header.Coinbase
	eth1Block.StateRoot = header.Root
	eth1Block.ReceiptsRoot = header.ReceiptHash
	eth1Block.LogsBloom = header.Bloom
	eth1Block.PrevRandao = header.MixDigest
	eth1Block.BlockNumber = header.Number.Uint64()
	eth1Block.GasLimit = header.GasLimit
	eth1Block.GasUsed = header.GasUsed
	eth1Block.Time = header.Time
	eth1Block.Extra = extraData
	eth1Block.BaseFeePerGas = baseFeeLE
	eth1Block.BlockHash = block.Hash()
	eth1Block.Transactions = solid.NewTransactionsSSZFromTransactions(encodedTxs)

	// Withdrawals
	eth1Block.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](
		int(beaconCfg.MaxWithdrawalsPerPayload), 44)
	for _, w := range block.Withdrawals() {
		eth1Block.Withdrawals.Append(&cltypes.Withdrawal{
			Amount:    w.Amount,
			Address:   w.Address,
			Index:     w.Index,
			Validator: w.Validator,
		})
	}

	if header.ExcessBlobGas != nil {
		eth1Block.ExcessBlobGas = *header.ExcessBlobGas
	}
	if header.BlobGasUsed != nil {
		eth1Block.BlobGasUsed = *header.BlobGasUsed
	}
	// SlotNumber is mandatory in GLOAS — ProcessExecutionPayloadEnvelope checks
	// payload.slot_number == state.slot (operations.go:778). The EL block builder
	// sets header.SlotNumber from the Parameters.SlotNumber we passed in.
	if header.SlotNumber != nil {
		eth1Block.SlotNumber = *header.SlotNumber
	}

	// Blob bundle from transactions
	engineBundle, err := engine_types.BlobsBundleFromTransactions(block.Transactions())
	if err != nil {
		return nil, fmt.Errorf("eladapter: blob bundle: %w", err)
	}
	var blobsBundle *BlobsBundle
	if engineBundle != nil {
		commitments := make([][]byte, len(engineBundle.Commitments))
		for i, c := range engineBundle.Commitments {
			commitments[i] = []byte(c)
		}
		proofs := make([][]byte, len(engineBundle.Proofs))
		for i, p := range engineBundle.Proofs {
			proofs[i] = []byte(p)
		}
		blobs := make([][]byte, len(engineBundle.Blobs))
		for i, b := range engineBundle.Blobs {
			blobs[i] = []byte(b)
		}
		blobsBundle = &BlobsBundle{
			Commitments: commitments,
			Proofs:      proofs,
			Blobs:       blobs,
		}
	}

	// Requests bundle
	var requestsBundle *typesproto.RequestsBundle
	if br.Requests != nil {
		requestsBundle = &typesproto.RequestsBundle{}
		for _, r := range br.Requests {
			requestsBundle.Requests = append(requestsBundle.Requests, r.Encode())
		}
	}

	return &AssembledPayload{
		Eth1Block:      eth1Block,
		BlobsBundle:    blobsBundle,
		RequestsBundle: requestsBundle,
		BlockValue:     blockValue,
	}, nil
}
