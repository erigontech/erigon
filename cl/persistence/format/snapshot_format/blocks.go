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

package snapshot_format

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

type ExecutionBlockReaderByNumber interface {
	Transactions(number uint64, hash common.Hash) (*solid.TransactionsSSZ, error)
	Withdrawals(number uint64, hash common.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error)
	SetBeaconChainConfig(beaconCfg *clparams.BeaconChainConfig)
}

var buffersPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

// WriteBlockForSnapshot writes a block to the given writer in the format expected by the snapshot.
// buf is just a reusable buffer. if it had to grow it will be returned back as grown.
func WriteBlockForSnapshot(w io.Writer, block *cltypes.SignedBeaconBlock, reusable []byte) ([]byte, error) {
	bodyRoot, err := block.Block.Body.HashSSZ()
	if err != nil {
		return reusable, err
	}
	reusable = reusable[:0]
	// Find the blinded block
	blinded, err := block.Blinded()
	if err != nil {
		return reusable, err
	}
	// Maybe reuse the buffer?
	encoded, err := blinded.EncodeSSZ(reusable)
	if err != nil {
		return reusable, err
	}

	reusable = encoded
	version := block.Version()
	if _, err := w.Write([]byte{byte(version)}); err != nil {
		return reusable, err
	}
	if _, err := w.Write(bodyRoot[:]); err != nil {
		return reusable, err
	}
	// Write the length of the buffer
	length := make([]byte, 8)
	binary.BigEndian.PutUint64(length, uint64(len(reusable)))
	if _, err := w.Write(length); err != nil {
		return reusable, err
	}
	// Write the buffer
	if _, err := w.Write(reusable); err != nil {
		return reusable, err
	}
	return reusable, nil
}

func readMetadataForBlock(r io.Reader, b []byte) (clparams.StateVersion, common.Hash, error) {
	if _, err := r.Read(b); err != nil {
		return 0, common.Hash{}, err
	}
	return clparams.StateVersion(b[0]), common.BytesToHash(b[1:]), nil
}

func ReadBlockFromSnapshot(r io.Reader, executionReader ExecutionBlockReaderByNumber, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, error) {
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	// Read the metadata
	metadataSlab := make([]byte, 33)
	v, _, err := readMetadataForBlock(r, metadataSlab)
	if err != nil {
		return nil, err
	}
	blindedBlock := cltypes.NewSignedBlindedBeaconBlock(cfg, v)
	// Read the length
	length := make([]byte, 8)
	if _, err := io.ReadFull(r, length); err != nil {
		return nil, err
	}
	// Read the block
	if _, err := io.CopyN(buffer, r, int64(binary.BigEndian.Uint64(length))); err != nil {
		return nil, err
	}
	// Decode the block in blinded
	if err := blindedBlock.DecodeSSZ(buffer.Bytes(), int(v)); err != nil {
		return nil, err
	}
	// No execution data for pre-altair blocks
	if v <= clparams.AltairVersion {
		return blindedBlock.Full(nil, nil), nil
	}
	blockNumber := blindedBlock.Block.Body.ExecutionPayload.BlockNumber
	blockHash := blindedBlock.Block.Body.ExecutionPayload.BlockHash
	txs, err := executionReader.Transactions(blockNumber, blockHash)
	if err != nil {
		return nil, err
	}
	ws, err := executionReader.Withdrawals(blockNumber, blockHash)
	if err != nil {
		return nil, err
	}
	return blindedBlock.Full(txs, ws), nil
}

// ReadBlockHeaderFromSnapshotWithExecutionData reads the beacon block header and the EL block number and block hash.
func ReadBlockHeaderFromSnapshotWithExecutionData(r io.Reader, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlockHeader, uint64, common.Hash, error) {
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	// Read the metadata
	metadataSlab := make([]byte, 33)
	v, bodyRoot, err := readMetadataForBlock(r, metadataSlab)
	if err != nil {
		return nil, 0, common.Hash{}, err
	}
	blindedBlock := cltypes.NewSignedBlindedBeaconBlock(cfg, v)

	// Read the length
	length := make([]byte, 8)
	if _, err := io.ReadFull(r, length); err != nil {
		return nil, 0, common.Hash{}, err
	}
	// Read the block
	if _, err := io.CopyN(buffer, r, int64(binary.BigEndian.Uint64(length))); err != nil {
		return nil, 0, common.Hash{}, err
	}
	// Decode the block in blinded
	if err := blindedBlock.DecodeSSZ(buffer.Bytes(), int(v)); err != nil {
		return nil, 0, common.Hash{}, err
	}
	blockHeader := &cltypes.SignedBeaconBlockHeader{
		Signature: blindedBlock.Signature,
		Header: &cltypes.BeaconBlockHeader{
			Slot:          blindedBlock.Block.Slot,
			ProposerIndex: blindedBlock.Block.ProposerIndex,
			ParentRoot:    blindedBlock.Block.ParentRoot,
			Root:          blindedBlock.Block.StateRoot,
			BodyRoot:      bodyRoot,
		},
	}
	// No execution data for pre-altair blocks
	if v <= clparams.AltairVersion {
		return blockHeader, 0, common.Hash{}, nil
	}
	blockNumber := blindedBlock.Block.Body.ExecutionPayload.BlockNumber
	blockHash := blindedBlock.Block.Body.ExecutionPayload.BlockHash
	return blockHeader, blockNumber, blockHash, nil
}

func ReadBlindedBlockFromSnapshot(r io.Reader, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBlindedBeaconBlock, error) {
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	// Read the metadata
	metadataSlab := make([]byte, 33)
	v, _, err := readMetadataForBlock(r, metadataSlab)
	if err != nil {
		return nil, err
	}
	blindedBlock := cltypes.NewSignedBlindedBeaconBlock(cfg, v)

	// Read the length
	length := make([]byte, 8)
	if _, err := io.ReadFull(r, length); err != nil {
		return nil, err
	}
	// Read the block
	if _, err := io.CopyN(buffer, r, int64(binary.BigEndian.Uint64(length))); err != nil {
		return nil, err
	}
	// Decode the block in blinded
	if err := blindedBlock.DecodeSSZ(buffer.Bytes(), int(v)); err != nil {
		return nil, err
	}
	return blindedBlock, nil
}
