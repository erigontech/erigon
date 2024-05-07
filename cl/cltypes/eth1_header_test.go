package cltypes

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/assert"
)

func TestEth1Header(t *testing.T) {
	version := clparams.DenebVersion

	header := NewEth1Header(clparams.BellatrixVersion)
	header = header.Copy()
	assert.True(t, header.IsZero())
	header.Capella()
	header.Deneb()
	// Create sample data
	parentHash := libcommon.Hash{}
	feeRecipient := libcommon.Address{}
	stateRoot := libcommon.Hash{}
	receiptsRoot := libcommon.Hash{}
	logsBloom := types.Bloom{}
	prevRandao := libcommon.Hash{}
	blockNumber := uint64(10)
	gasLimit := uint64(20)
	gasUsed := uint64(30)
	time := uint64(40)
	extra := solid.NewExtraData()
	baseFeePerGas := [32]byte{}
	blockHash := libcommon.Hash{}
	transactionsRoot := libcommon.Hash{}
	withdrawalsRoot := libcommon.Hash{}
	blobGasUsed := uint64(50)
	excessBlobGas := uint64(60)

	// Test Eth1Header
	header = &Eth1Header{
		ParentHash:       parentHash,
		FeeRecipient:     feeRecipient,
		StateRoot:        stateRoot,
		ReceiptsRoot:     receiptsRoot,
		LogsBloom:        logsBloom,
		PrevRandao:       prevRandao,
		BlockNumber:      blockNumber,
		GasLimit:         gasLimit,
		GasUsed:          gasUsed,
		Time:             time,
		Extra:            extra,
		BaseFeePerGas:    baseFeePerGas,
		BlockHash:        blockHash,
		TransactionsRoot: transactionsRoot,
		WithdrawalsRoot:  withdrawalsRoot,
		BlobGasUsed:      blobGasUsed,
		ExcessBlobGas:    excessBlobGas,
		version:          version,
	}

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := header.EncodeSSZ(nil)
	assert.NoError(t, err)
	decodedHeader := &Eth1Header{}
	err = decodedHeader.DecodeSSZ(encodedData, int(version))
	assert.NoError(t, err)
	assert.Equal(t, header, decodedHeader)

	// Test EncodingSizeSSZ
	expectedEncodingSize := header.EncodingSizeSSZ()
	encodingSize := header.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test HashSSZ
	root, err := header.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.HexToHash("0x9170a25a0980f07bcb9af2a52ff915262763e0e6a2df26aa205b967bd462a6d3"), libcommon.Hash(root))
}
