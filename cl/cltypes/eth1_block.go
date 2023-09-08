package cltypes

import (
	"fmt"
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/core/types"
)

// ETH1Block represents a block structure CL-side.
type Eth1Block struct {
	ParentHash    libcommon.Hash    `json:"parent_hash"`
	FeeRecipient  libcommon.Address `json:"fee_recipient"`
	StateRoot     libcommon.Hash    `json:"state_root"`
	ReceiptsRoot  libcommon.Hash    `json:"receipts_root"`
	LogsBloom     types.Bloom       `json:"logs_bloom"`
	PrevRandao    libcommon.Hash    `json:"prev_randao"`
	BlockNumber   uint64            `json:"block_number"`
	GasLimit      uint64            `json:"gas_limit"`
	GasUsed       uint64            `json:"gas_used"`
	Time          uint64            `json:"timestamp"`
	Extra         *solid.ExtraData  `json:"extra_data"`
	BaseFeePerGas libcommon.Hash    `json:"base_fee_per_gas"`
	// Extra fields
	BlockHash     libcommon.Hash                    `json:"block_hash"`
	Transactions  *solid.TransactionsSSZ            `json:"transactions"`
	Withdrawals   *solid.ListSSZ[*types.Withdrawal] `json:"withdrawals,omitempty"`
	BlobGasUsed   uint64                            `json:"blob_gas_used,omitempty"`
	ExcessBlobGas uint64                            `json:"excess_blob_gas,omitempty"`
	// internals
	version   clparams.StateVersion
	beaconCfg *clparams.BeaconChainConfig
}

// NewEth1Block creates a new Eth1Block.
func NewEth1Block(version clparams.StateVersion, beaconCfg *clparams.BeaconChainConfig) *Eth1Block {
	return &Eth1Block{version: version, beaconCfg: beaconCfg}
}

// NewEth1BlockFromHeaderAndBody with given header/body.
func NewEth1BlockFromHeaderAndBody(header *types.Header, body *types.RawBody, beaconCfg *clparams.BeaconChainConfig) *Eth1Block {
	baseFeeBytes := header.BaseFee.Bytes()
	for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
		baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
	}
	var baseFee32 [32]byte
	copy(baseFee32[:], baseFeeBytes)

	extra := solid.NewExtraData()
	extra.SetBytes(header.Extra)
	block := &Eth1Block{
		ParentHash:    header.ParentHash,
		FeeRecipient:  header.Coinbase,
		StateRoot:     header.Root,
		ReceiptsRoot:  header.ReceiptHash,
		LogsBloom:     header.Bloom,
		PrevRandao:    header.MixDigest,
		BlockNumber:   header.Number.Uint64(),
		GasLimit:      header.GasLimit,
		GasUsed:       header.GasUsed,
		Time:          header.Time,
		Extra:         extra,
		BaseFeePerGas: baseFee32,
		BlockHash:     header.Hash(),
		Transactions:  solid.NewTransactionsSSZFromTransactions(body.Transactions),
		Withdrawals:   solid.NewStaticListSSZFromList(body.Withdrawals, int(beaconCfg.MaxWithdrawalsPerPayload), 44),
		beaconCfg:     beaconCfg,
	}

	if header.BlobGasUsed != nil && header.ExcessBlobGas != nil {
		block.BlobGasUsed = *header.BlobGasUsed
		block.ExcessBlobGas = *header.ExcessBlobGas
		block.version = clparams.DenebVersion
	} else if header.WithdrawalsHash != nil {
		block.version = clparams.CapellaVersion
	} else {
		block.version = clparams.BellatrixVersion
	}
	return block
}

func (*Eth1Block) Static() bool {
	return false
}

// PayloadHeader returns the equivalent ExecutionPayloadHeader object.
func (b *Eth1Block) PayloadHeader() (*Eth1Header, error) {
	var err error
	var transactionsRoot, withdrawalsRoot libcommon.Hash
	if transactionsRoot, err = b.Transactions.HashSSZ(); err != nil {
		return nil, err
	}
	if b.version >= clparams.CapellaVersion {
		withdrawalsRoot, err = b.Withdrawals.HashSSZ()
		if err != nil {
			return nil, err
		}
	}

	var blobGasUsed, excessBlobGas uint64
	if b.version >= clparams.DenebVersion {
		blobGasUsed = b.BlobGasUsed
		excessBlobGas = b.ExcessBlobGas
	}

	return &Eth1Header{
		ParentHash:       b.ParentHash,
		FeeRecipient:     b.FeeRecipient,
		StateRoot:        b.StateRoot,
		ReceiptsRoot:     b.ReceiptsRoot,
		LogsBloom:        b.LogsBloom,
		PrevRandao:       b.PrevRandao,
		BlockNumber:      b.BlockNumber,
		GasLimit:         b.GasLimit,
		GasUsed:          b.GasUsed,
		Time:             b.Time,
		Extra:            b.Extra,
		BaseFeePerGas:    b.BaseFeePerGas,
		BlockHash:        b.BlockHash,
		TransactionsRoot: transactionsRoot,
		WithdrawalsRoot:  withdrawalsRoot,
		BlobGasUsed:      blobGasUsed,
		ExcessBlobGas:    excessBlobGas,
		version:          b.version,
	}, nil
}

// Return minimum required buffer length to be an acceptable SSZ encoding.
func (b *Eth1Block) EncodingSizeSSZ() (size int) {
	size = 508
	if b.Extra == nil {
		b.Extra = solid.NewExtraData()
	}
	// Field (10) 'ExtraData'
	size += b.Extra.EncodingSizeSSZ()
	// Field (13) 'Transactions'
	size += b.Transactions.EncodingSizeSSZ()

	if b.version >= clparams.CapellaVersion {
		if b.Withdrawals == nil {
			b.Withdrawals = solid.NewStaticListSSZ[*types.Withdrawal](int(b.beaconCfg.MaxWithdrawalsPerPayload), 44)
		}
		size += b.Withdrawals.EncodingSizeSSZ() + 4
	}

	if b.version >= clparams.DenebVersion {
		size += 8 * 2 // BlobGasUsed + ExcessBlobGas
	}

	return
}

// DecodeSSZ decodes the block in SSZ format.
func (b *Eth1Block) DecodeSSZ(buf []byte, version int) error {
	b.Extra = solid.NewExtraData()
	b.Transactions = &solid.TransactionsSSZ{}
	b.Withdrawals = solid.NewStaticListSSZ[*types.Withdrawal](int(b.beaconCfg.MaxWithdrawalsPerPayload), 44)
	b.version = clparams.StateVersion(version)
	return ssz2.UnmarshalSSZ(buf, version, b.getSchema()...)
}

// EncodeSSZ encodes the block in SSZ format.
func (b *Eth1Block) EncodeSSZ(dst []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(dst, b.getSchema()...)
}

// HashSSZ calculates the SSZ hash of the Eth1Block's payload header.
func (b *Eth1Block) HashSSZ() ([32]byte, error) {
	return merkle_tree.HashTreeRoot(b.getSchema()...)
}

func (b *Eth1Block) getSchema() []interface{} {
	s := []interface{}{b.ParentHash[:], b.FeeRecipient[:], b.StateRoot[:], b.ReceiptsRoot[:], b.LogsBloom[:],
		b.PrevRandao[:], &b.BlockNumber, &b.GasLimit, &b.GasUsed, &b.Time, b.Extra, b.BaseFeePerGas[:], b.BlockHash[:], b.Transactions}
	if b.version >= clparams.CapellaVersion {
		s = append(s, b.Withdrawals)
	}
	if b.version >= clparams.DenebVersion {
		s = append(s, &b.BlobGasUsed, &b.ExcessBlobGas)
	}
	return s
}

// RlpHeader returns the equivalent types.Header struct with RLP-based fields.
func (b *Eth1Block) RlpHeader() (*types.Header, error) {
	// Reverse the order of the bytes in the BaseFeePerGas array and convert it to a big integer.
	reversedBaseFeePerGas := libcommon.Copy(b.BaseFeePerGas[:])
	for i, j := 0, len(reversedBaseFeePerGas)-1; i < j; i, j = i+1, j-1 {
		reversedBaseFeePerGas[i], reversedBaseFeePerGas[j] = reversedBaseFeePerGas[j], reversedBaseFeePerGas[i]
	}
	baseFee := new(big.Int).SetBytes(reversedBaseFeePerGas)
	// If the block version is Capella or later, calculate the withdrawals hash.
	var withdrawalsHash *libcommon.Hash
	if b.version >= clparams.CapellaVersion {
		withdrawalsHash = new(libcommon.Hash)
		// extract all withdrawals from itearable list
		withdrawals := make([]*types.Withdrawal, b.Withdrawals.Len())
		b.Withdrawals.Range(func(idx int, w *types.Withdrawal, _ int) bool {
			withdrawals[idx] = w
			return true
		})
		*withdrawalsHash = types.DeriveSha(types.Withdrawals(withdrawals))
	}

	header := &types.Header{
		ParentHash:      b.ParentHash,
		UncleHash:       types.EmptyUncleHash,
		Coinbase:        b.FeeRecipient,
		Root:            b.StateRoot,
		TxHash:          types.DeriveSha(types.BinaryTransactions(b.Transactions.UnderlyngReference())),
		ReceiptHash:     b.ReceiptsRoot,
		Bloom:           b.LogsBloom,
		Difficulty:      merge.ProofOfStakeDifficulty,
		Number:          big.NewInt(int64(b.BlockNumber)),
		GasLimit:        b.GasLimit,
		GasUsed:         b.GasUsed,
		Time:            b.Time,
		Extra:           b.Extra.Bytes(),
		MixDigest:       b.PrevRandao,
		Nonce:           merge.ProofOfStakeNonce,
		BaseFee:         baseFee,
		WithdrawalsHash: withdrawalsHash,
	}

	if b.version >= clparams.DenebVersion {
		blobGasUsed := b.BlobGasUsed
		header.BlobGasUsed = &blobGasUsed
		excessBlobGas := b.ExcessBlobGas
		header.ExcessBlobGas = &excessBlobGas
	}

	// If the header hash does not match the block hash, return an error.
	if header.Hash() != b.BlockHash {
		return nil, fmt.Errorf("cannot derive rlp header: mismatching hash: %s != %s", header.Hash(), b.BlockHash)
	}

	return header, nil
}

func (b *Eth1Block) Version() clparams.StateVersion {
	return b.version
}

// Body returns the equivalent raw body (only eth1 body section).
func (b *Eth1Block) Body() *types.RawBody {
	withdrawals := make([]*types.Withdrawal, b.Withdrawals.Len())
	b.Withdrawals.Range(func(idx int, w *types.Withdrawal, _ int) bool {
		withdrawals[idx] = w
		return true
	})
	return &types.RawBody{
		Transactions: b.Transactions.UnderlyngReference(),
		Withdrawals:  types.Withdrawals(withdrawals),
	}
}
