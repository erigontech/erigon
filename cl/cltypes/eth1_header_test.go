// Copyright 2024 The Erigon Authors
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

package cltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/execution/types"
)

func TestEth1Header(t *testing.T) {
	version := clparams.DenebVersion

	header := NewEth1Header(clparams.BellatrixVersion)
	header = header.Copy()
	assert.True(t, header.IsZero())
	header.Capella()
	header.Deneb()
	// Create sample data
	parentHash := common.Hash{}
	feeRecipient := common.Address{}
	stateRoot := common.Hash{}
	receiptsRoot := common.Hash{}
	logsBloom := types.Bloom{}
	prevRandao := common.Hash{}
	blockNumber := uint64(10)
	gasLimit := uint64(20)
	gasUsed := uint64(30)
	time := uint64(40)
	extra := solid.NewExtraData()
	baseFeePerGas := [32]byte{}
	blockHash := common.Hash{}
	transactionsRoot := common.Hash{}
	withdrawalsRoot := common.Hash{}
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
	require.NoError(t, err)
	decodedHeader := &Eth1Header{}
	err = decodedHeader.DecodeSSZ(encodedData, int(version))
	require.NoError(t, err)
	assert.Equal(t, header, decodedHeader)

	// Test EncodingSizeSSZ
	expectedEncodingSize := header.EncodingSizeSSZ()
	encodingSize := header.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test HashSSZ
	root, err := header.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, common.HexToHash("0x9170a25a0980f07bcb9af2a52ff915262763e0e6a2df26aa205b967bd462a6d3"), common.Hash(root))
}
