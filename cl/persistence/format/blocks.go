package format

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

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
	temp := make([]byte, 4)
	binary.BigEndian.PutUint64(temp, p.BlockNumber)

	return writeChunk(w, temp, pointerDataType)
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
	if err := writeChunk(w, encoded[:currentChunkLength], chunkDataType); err != nil {
		return err
	}
	if err := writeChunk(w, EncodeAttestationsForStorage(body.Attestations, nil), chunkDataType); err != nil {
		return err
	}
	encoded = encoded[:currentChunkLength+uint64(body.Attestations.EncodingSizeSSZ())]
	currentChunkLength = 0

	currentChunkLength += uint64(body.Deposits.EncodingSizeSSZ())
	currentChunkLength += uint64(body.VoluntaryExits.EncodingSizeSSZ())

	if err := writeChunk(w, encoded[:currentChunkLength], chunkDataType); err != nil {
		return err
	}
	// we are done if we are before altair
	if version <= clparams.AltairVersion {
		return nil
	}
	encoded = encoded[:currentChunkLength+uint64(body.ExecutionPayload.EncodingSizeSSZ())]
	currentChunkLength = 0
	if err := writeExecutionBlockPtr(w, body.ExecutionPayload); err != nil {
		return err
	}
	if version <= clparams.BellatrixVersion {
		return nil
	}
	if err := writeExecutionBlockPtr(w, body.ExecutionPayload); err != nil {
		return err
	}
	currentChunkLength += uint64(body.ExecutionChanges.EncodingSizeSSZ())
	if version >= clparams.DenebVersion {
		currentChunkLength += uint64(body.BlobKzgCommitments.EncodingSizeSSZ())
	}
	return writeChunk(w, encoded, chunkDataType)
}

func ReadBlockFromrSnapshot(r io.Reader, cfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, error) {
	// Metadata section is just the current hardfork of the block.
	version, err := readMetadataForBlock(r)
	if err != nil {
		return nil, err
	}
	// we have 4 chunks in total.
	block := cltypes.NewSignedBeaconBlock(cfg)

	// Read the first chunk
	chunk1Bytes, dT1, err := readChunk(r)
	if err != nil {
		return nil, err
	}
	if dT1 != chunkDataType {
		return nil, fmt.Errorf("malformed beacon block, invalid chunk type %d, expected: %d", dT1, chunkDataType)
	}
	// Read the attestation chunk (2nd chunk)
	attestantionChunk, dT2, err := readChunk(r)
	if err != nil {
		return nil, err
	}
	if dT2 != chunkDataType {
		return nil, fmt.Errorf("malformed beacon block, invalid chunk type %d, expected: %d", dT2, chunkDataType)
	}

}
