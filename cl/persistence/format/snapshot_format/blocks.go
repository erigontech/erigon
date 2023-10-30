package snapshot_format

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/format/chunk_encoding"
)

var buffersPool = sync.Pool{
	New: func() interface{} { return &bytes.Buffer{} },
}

type ExecutionBlockReaderByNumber interface {
	BlockByNumber(number uint64) (*cltypes.Eth1Block, error)
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
	temp := make([]byte, 8)
	binary.BigEndian.PutUint64(temp, p.BlockNumber)

	return chunk_encoding.WriteChunk(w, temp, chunk_encoding.PointerDataType)
}

func readExecutionBlockPtr(r io.Reader) (uint64, error) {
	b, dT, err := chunk_encoding.ReadChunkToBytes(r)
	if err != nil {
		return 0, err
	}
	if dT != chunk_encoding.PointerDataType {
		return 0, fmt.Errorf("malformed beacon block, invalid block pointer type %d, expected: %d", dT, chunk_encoding.ChunkDataType)
	}
	return binary.BigEndian.Uint64(b), nil
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
func WriteBlockForSnapshot(block *cltypes.SignedBeaconBlock, w io.Writer) error {
	bodyRoot, err := block.Block.Body.HashSSZ()
	if err != nil {
		return err
	}
	// Maybe reuse the buffer?
	encoded, err := block.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	version := block.Version()
	if _, err := w.Write([]byte{byte(version)}); err != nil {
		return err
	}
	if _, err := w.Write(bodyRoot[:]); err != nil {
		return err
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
		return err
	}
	// we are done if we are before altair
	if version <= clparams.AltairVersion {
		return nil
	}
	encoded = encoded[currentChunkLength+uint64(body.ExecutionPayload.EncodingSizeSSZ()):]
	if err := writeExecutionBlockPtr(w, body.ExecutionPayload); err != nil {
		return err
	}
	if version <= clparams.BellatrixVersion {
		return nil
	}
	return chunk_encoding.WriteChunk(w, encoded, chunk_encoding.ChunkDataType)
}

func readMetadataForBlock(r io.Reader, b []byte) (clparams.StateVersion, error) {
	if _, err := r.Read(b); err != nil {
		return 0, err
	}
	return clparams.StateVersion(b[0]), nil
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

func ReadRawBlockFromSnapshot(r io.Reader, out io.Writer, executionReader ExecutionBlockReaderByNumber, cfg *clparams.BeaconChainConfig) (clparams.StateVersion, error) {
	metadataSlab := make([]byte, 33)
	// Metadata section is just the current hardfork of the block. TODO(give it a useful purpose)
	v, err := readMetadataForBlock(r, metadataSlab)
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
	blockPointer, err := readExecutionBlockPtr(r)
	if err != nil {
		return v, err
	}
	executionBlock, err := executionReader.BlockByNumber(blockPointer)
	if err != nil {
		return v, err
	}
	if executionBlock == nil {
		return v, fmt.Errorf("execution block %d not found", blockPointer)
	}
	// TODO(Giulio2002): optimize GC
	eth1Bytes, err := executionBlock.EncodeSSZ(nil)
	if err != nil {
		return v, err
	}
	if _, err := out.Write(eth1Bytes); err != nil {
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
