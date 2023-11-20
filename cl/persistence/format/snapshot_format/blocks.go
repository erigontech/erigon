package snapshot_format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/format/chunk_encoding"
)

type ExecutionBlockReaderByNumber interface {
	TransactionsSSZ(w io.Writer, number uint64, hash libcommon.Hash) error
	WithdrawalsSZZ(w io.Writer, number uint64, hash libcommon.Hash) error
}

var buffersPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

const (
	blockBaseOffset = 100 /* Signature + Block Offset */ +
		84 /* Slot + ProposerIndex + ParentRoot + StateRoot + Body Offset */ +
		96 /*Signature*/ + 72 /*Eth1Data*/ + 32 /*Graffiti*/ + 4 /*ProposerSlashings Offset*/ + 4 /*AttesterSlashings Offset*/ + 4 /*Attestations*/ +
		4 /*Deposits Offset*/ + 4 /*VoluntaryExits Offset*/

	altairBlockAdditionalBaseOffset    = 160 /*SyncAggregate*/
	bellatrixBlockAdditionalBaseOffset = 4   /*ExecutionPayload Offset*/
	capellaBlockAdditionalBaseOffset   = 4   /*ExecutionChanges Offset*/
	denebBlockAdditionalBaseOffset     = 4   /*BlobKzgCommitments Offset*/
)

func writeExecutionBlockPtr(w io.Writer, p *cltypes.Eth1Block) error {
	temp := make([]byte, 40)
	binary.BigEndian.PutUint64(temp, p.BlockNumber)
	copy(temp[8:], p.BlockHash[:])

	return chunk_encoding.WriteChunk(w, temp, chunk_encoding.PointerDataType)
}

func readExecutionBlockPtr(r io.Reader) (uint64, libcommon.Hash, error) {
	b, dT, err := chunk_encoding.ReadChunkToBytes(r)
	if err != nil {
		return 0, libcommon.Hash{}, err
	}
	if dT != chunk_encoding.PointerDataType {
		return 0, libcommon.Hash{}, fmt.Errorf("malformed beacon block, invalid block pointer type %d, expected: %d", dT, chunk_encoding.ChunkDataType)
	}
	return binary.BigEndian.Uint64(b[:8]), libcommon.BytesToHash(b[8:]), nil
}

func computeInitialOffset(version clparams.StateVersion) uint64 {
	ret := uint64(blockBaseOffset)
	if version >= clparams.AltairVersion {
		ret += altairBlockAdditionalBaseOffset
	}
	if version >= clparams.BellatrixVersion {
		ret += bellatrixBlockAdditionalBaseOffset
	}
	if version >= clparams.CapellaVersion {
		ret += capellaBlockAdditionalBaseOffset
	}
	if version >= clparams.DenebVersion {
		ret += denebBlockAdditionalBaseOffset
	}
	return ret
}

// WriteBlockForSnapshot writes a block to the given writer in the format expected by the snapshot.
// buf is just a reusable buffer. if it had to grow it will be returned back as grown.
func WriteBlockForSnapshot(w io.Writer, block *cltypes.SignedBeaconBlock, reusable []byte) ([]byte, error) {
	bodyRoot, err := block.Block.Body.HashSSZ()
	if err != nil {
		return reusable, err
	}
	reusable = reusable[:0]
	// Maybe reuse the buffer?
	encoded, err := block.EncodeSSZ(reusable)
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
	currentChunkLength := computeInitialOffset(version)

	body := block.Block.Body
	// count in body for phase0 fields
	currentChunkLength += uint64(body.ProposerSlashings.EncodingSizeSSZ())
	currentChunkLength += uint64(body.AttesterSlashings.EncodingSizeSSZ())
	currentChunkLength += uint64(body.Attestations.EncodingSizeSSZ())
	currentChunkLength += uint64(body.Deposits.EncodingSizeSSZ())
	currentChunkLength += uint64(body.VoluntaryExits.EncodingSizeSSZ())
	// Write the chunk and chunk attestations
	if err := chunk_encoding.WriteChunk(w, encoded[:currentChunkLength], chunk_encoding.ChunkDataType); err != nil {
		return reusable, err
	}
	// we are done if we are before altair
	if version <= clparams.AltairVersion {
		return reusable, nil
	}
	encoded = encoded[currentChunkLength:]
	if err := writeEth1BlockForSnapshot(w, encoded[:body.ExecutionPayload.EncodingSizeSSZ()], body.ExecutionPayload); err != nil {
		return reusable, err
	}
	encoded = encoded[body.ExecutionPayload.EncodingSizeSSZ():]
	if version <= clparams.BellatrixVersion {
		return reusable, nil
	}
	return reusable, chunk_encoding.WriteChunk(w, encoded, chunk_encoding.ChunkDataType)
}

func readMetadataForBlock(r io.Reader, b []byte) (clparams.StateVersion, libcommon.Hash, error) {
	if _, err := r.Read(b); err != nil {
		return 0, libcommon.Hash{}, err
	}
	return clparams.StateVersion(b[0]), libcommon.BytesToHash(b[1:]), nil
}

func ReadBlockFromSnapshot(r io.Reader, executionReader ExecutionBlockReaderByNumber, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, error) {
	block := cltypes.NewSignedBeaconBlock(cfg)
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	v, err := ReadRawBlockFromSnapshot(r, buffer, executionReader, cfg)
	if err != nil {
		return nil, err
	}
	return block, block.DecodeSSZ(buffer.Bytes(), int(v))
}

// ReadBlockHeaderFromSnapshotWithExecutionData reads the beacon block header and the EL block number and block hash.
func ReadBlockHeaderFromSnapshotWithExecutionData(r io.Reader) (*cltypes.SignedBeaconBlockHeader, uint64, libcommon.Hash, error) {
	buffer := buffersPool.Get().(*bytes.Buffer)
	defer buffersPool.Put(buffer)
	buffer.Reset()

	metadataSlab := make([]byte, 33)
	v, bodyRoot, err := readMetadataForBlock(r, metadataSlab)
	if err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	chunk1, dT1, err := chunk_encoding.ReadChunkToBytes(r)
	if err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	if dT1 != chunk_encoding.ChunkDataType {
		return nil, 0, libcommon.Hash{}, fmt.Errorf("malformed beacon block, invalid chunk 1 type %d, expected: %d", dT1, chunk_encoding.ChunkDataType)
	}

	var signature libcommon.Bytes96
	copy(signature[:], chunk1[4:100])
	header := &cltypes.SignedBeaconBlockHeader{
		Signature: signature,
		Header: &cltypes.BeaconBlockHeader{
			Slot:          binary.LittleEndian.Uint64(chunk1[100:108]),
			ProposerIndex: binary.LittleEndian.Uint64(chunk1[108:116]),
			ParentRoot:    libcommon.BytesToHash(chunk1[116:148]),
			Root:          libcommon.BytesToHash(chunk1[148:180]),
			BodyRoot:      bodyRoot,
		}}
	if v <= clparams.AltairVersion {
		return header, 0, libcommon.Hash{}, nil
	}
	if _, err := r.Read(make([]byte, 1)); err != nil {
		return header, 0, libcommon.Hash{}, nil
	}
	// Read the first eth 1 block chunk
	_, err = chunk_encoding.ReadChunk(r, io.Discard)
	if err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	// lastly read the executionBlock ptr
	blockNumber, blockHash, err := readExecutionBlockPtr(r)
	if err != nil {
		return nil, 0, libcommon.Hash{}, err
	}
	return header, blockNumber, blockHash, nil
}

func ReadRawBlockFromSnapshot(r io.Reader, out io.Writer, executionReader ExecutionBlockReaderByNumber, cfg *clparams.BeaconChainConfig) (clparams.StateVersion, error) {
	metadataSlab := make([]byte, 33)
	// Metadata section is just the current hardfork of the block.
	v, _, err := readMetadataForBlock(r, metadataSlab)
	if err != nil {
		return v, err
	}

	// Read the first chunk
	dT1, err := chunk_encoding.ReadChunk(r, out)
	if err != nil {
		return v, err
	}
	if dT1 != chunk_encoding.ChunkDataType {
		return v, fmt.Errorf("malformed beacon block, invalid chunk 1 type %d, expected: %d", dT1, chunk_encoding.ChunkDataType)
	}

	if v <= clparams.AltairVersion {
		return v, nil
	}
	// Read the block pointer and retrieve chunk4 from the execution reader
	if _, err := readEth1BlockFromSnapshot(r, out, executionReader, cfg); err != nil {
		return v, err
	}
	if v <= clparams.BellatrixVersion {
		return v, nil
	}

	// Read the 5h chunk
	dT2, err := chunk_encoding.ReadChunk(r, out)
	if err != nil {
		return v, err
	}
	if dT2 != chunk_encoding.ChunkDataType {
		return v, fmt.Errorf("malformed beacon block, invalid chunk 5 type %d, expected: %d", dT2, chunk_encoding.ChunkDataType)
	}
	return v, nil
}
