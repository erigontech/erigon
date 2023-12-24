package snapshot_format

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

type ExecutionBlockReaderByNumber interface {
	Transactions(number uint64, hash libcommon.Hash) (*solid.TransactionsSSZ, error)
	Withdrawals(number uint64, hash libcommon.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error)
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

func readMetadataForBlock(r io.Reader, b []byte) (clparams.StateVersion, libcommon.Hash, error) {
	if _, err := r.Read(b); err != nil {
		return 0, libcommon.Hash{}, err
	}
	return clparams.StateVersion(b[0]), libcommon.BytesToHash(b[1:]), nil
}

func ReadBlockFromSnapshot(r io.Reader, executionReader ExecutionBlockReaderByNumber, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, error) {
	blindedBlock := cltypes.NewSignedBlindedBeaconBlock(cfg)
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	// Read the metadata
	metadataSlab := make([]byte, 33)
	v, _, err := readMetadataForBlock(r, metadataSlab)
	if err != nil {
		return nil, err
	}
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
func ReadBlockHeaderFromSnapshotWithExecutionData(r io.Reader, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlockHeader, uint64, libcommon.Hash, error) {
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	blindedBlock := cltypes.NewSignedBlindedBeaconBlock(cfg)

	// Read the metadata
	metadataSlab := make([]byte, 33)
	v, bodyRoot, err := readMetadataForBlock(r, metadataSlab)
	if err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	// Read the length
	length := make([]byte, 8)
	if _, err := io.ReadFull(r, length); err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	// Read the block
	if _, err := io.CopyN(buffer, r, int64(binary.BigEndian.Uint64(length))); err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	// Decode the block in blinded
	if err := blindedBlock.DecodeSSZ(buffer.Bytes(), int(v)); err != nil {
		return nil, 0, libcommon.Hash{}, err
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
		return blockHeader, 0, libcommon.Hash{}, nil
	}
	blockNumber := blindedBlock.Block.Body.ExecutionPayload.BlockNumber
	blockHash := blindedBlock.Block.Body.ExecutionPayload.BlockHash
	return blockHeader, blockNumber, blockHash, nil
}
