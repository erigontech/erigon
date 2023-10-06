package snapshot_format

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

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

func writeChunkLength(w io.Writer, length uint64) error {
	temp := make([]byte, 8)
	binary.BigEndian.PutUint64(temp, length)

	if _, err := w.Write(temp); err != nil {
		return err
	}

	return nil
}

func writeExecutionBlockPtr(w io.Writer, p *cltypes.Eth1Block) error {
	temp := make([]byte, 8)
	binary.BigEndian.PutUint64(temp, p.BlockNumber)

	return writeChunk(w, temp, pointerDataType, false)
}

func readExecutionBlockPtr(r io.Reader) (uint64, error) {
	b, dT, err := readChunk(r, false)
	if err != nil {
		return 0, err
	}
	if dT != pointerDataType {
		return 0, fmt.Errorf("malformed beacon block, invalid block pointer type %d, expected: %d", dT, pointerDataType)
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
	// Maybe reuse the buffer?
	encoded, err := block.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	version := block.Version()
	if _, err := w.Write([]byte{byte(version)}); err != nil {
		return err
	}
	currentChunkLength := computeInitialOffset(version)

	body := block.Block.Body
	// count in body for phase0 fields
	currentChunkLength += uint64(body.ProposerSlashings.EncodingSizeSSZ())
	currentChunkLength += uint64(body.AttesterSlashings.EncodingSizeSSZ())

	// Write the chunk and chunk attestations
	if err := writeChunk(w, encoded[:currentChunkLength], chunkDataType, false); err != nil {
		return err
	}
	encoded = encoded[currentChunkLength:]
	if err := writeChunk(w, encoded[:uint64(body.Attestations.EncodingSizeSSZ())], chunkDataType, true); err != nil {
		return err
	}
	encoded = encoded[body.Attestations.EncodingSizeSSZ():]
	currentChunkLength = 0

	currentChunkLength += uint64(body.Deposits.EncodingSizeSSZ())
	currentChunkLength += uint64(body.VoluntaryExits.EncodingSizeSSZ())

	if err := writeChunk(w, encoded[:currentChunkLength], chunkDataType, false); err != nil {
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
	return writeChunk(w, encoded, chunkDataType, false)
}

func ReadBlockFromrSnapshot(r io.Reader, executionReader ExecutionBlockReaderByNumber, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, error) {
	// Metadata section is just the current hardfork of the block. TODO(give it a useful purpose)
	v, err := readMetadataForBlock(r)
	if err != nil {
		return nil, err
	}

	// Read the first chunk
	chunk1, dT1, err := readChunk(r, false)
	if err != nil {
		return nil, err
	}
	if dT1 != chunkDataType {
		return nil, fmt.Errorf("malformed beacon block, invalid chunk 1 type %d, expected: %d", dT1, chunkDataType)
	}
	// Read the attestation chunk (2nd chunk)
	chunk2, dT2, err := readChunk(r, true)
	if err != nil {
		return nil, err
	}
	if dT2 != chunkDataType {
		return nil, fmt.Errorf("malformed beacon block, invalid chunk 2 type %d, expected: %d", dT2, chunkDataType)
	}
	// Read the 3rd chunk
	chunk3, dT3, err := readChunk(r, false)
	if err != nil {
		return nil, err
	}
	if dT3 != chunkDataType {
		return nil, fmt.Errorf("malformed beacon block, invalid chunk 3 type %d, expected: %d", dT3, chunkDataType)
	}
	if v <= clparams.AltairVersion {
		return blockFromChunks(v, cfg, chunk1, chunk2, chunk3)
	}
	// Read the block pointer and retrieve chunk4 from the execution reader
	blockPointer, err := readExecutionBlockPtr(r)
	if err != nil {
		return nil, err
	}
	executionBlock, err := executionReader.BlockByNumber(blockPointer)
	if err != nil {
		return nil, err
	}
	// Read the 4th chunk
	chunk4, err := executionBlock.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	if v <= clparams.BellatrixVersion {
		return blockFromChunks(v, cfg, chunk1, chunk2, chunk3, chunk4)
	}

	// Read the 5h chunk
	chunk5, dT5, err := readChunk(r, false)
	if err != nil {
		return nil, err
	}
	if dT5 != chunkDataType {
		return nil, fmt.Errorf("malformed beacon block, invalid chunk 5 type %d, expected: %d", dT5, chunkDataType)
	}

	return blockFromChunks(v, cfg, chunk1, chunk2, chunk3, chunk4, chunk5)
}

func blockFromChunks(v clparams.StateVersion, cfg *clparams.BeaconChainConfig, chunks ...[]byte) (*cltypes.SignedBeaconBlock, error) {
	block := cltypes.NewSignedBeaconBlock(cfg)
	plainSSZ := []byte{}
	for _, chunk := range chunks {
		plainSSZ = append(plainSSZ, chunk...)
	}
	return block, block.DecodeSSZ(plainSSZ, int(v))

}
