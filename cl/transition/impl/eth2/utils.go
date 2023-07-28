package eth2

import (
	"encoding/binary"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/core/types"
)

const VERSIONED_HASH_VERSION_KZG byte = byte(1)

func kzgCommitmentToVersionedHash(kzgCommitment *cltypes.KZGCommitment) (libcommon.Hash, error) {
	versionedHash := [32]byte{}
	kzgCommitmentHash := utils.Keccak256(kzgCommitment[:])

	buf := append([]byte{}, VERSIONED_HASH_VERSION_KZG)
	buf = append(buf, kzgCommitmentHash[1:]...)
	copy(versionedHash[:], buf)

	return versionedHash, nil
}

func txPeekBlobVersionedHashes(txBytes []byte) []libcommon.Hash {
	if txBytes[0] != types.BlobTxType {
		return []libcommon.Hash{}
	}

	messageOffset := 1 + binary.LittleEndian.Uint32(txBytes[1:5])

	/*
		https://gist.github.com/protolambda/23bd106b66f6d4bb854ce46044aa3ca3
		chain_id: 32 bytes
		nonce: 8 bytes
		priority_fee_per_gas: 32 bytes
		max_basefee_per_gas: 32 bytes
		gas: 8 bytes
		to: 4 bytes - offset to B (relative to A)
		value: 32 bytes
		data: 4 bytes - offset to C (relative to A)
		access_list: 4 bytes - offset to D (relative to A)
		max_fee_per_blob_gas: 32 bytes
		blob_versioned_hashes: 4 bytes - offset to E (relative to A)
	*/
	// field offset: 32 + 8 + 32 + 32 + 8 + 4 + 32 + 4 + 4 + 32 = 188
	blobVersionedHashes := messageOffset + binary.LittleEndian.Uint32(txBytes[messageOffset+188:messageOffset+192])

	versionedHashes := make([]libcommon.Hash, len(txBytes[blobVersionedHashes:])/32)
	for pos, i := blobVersionedHashes, 0; int(pos) < len(txBytes) && i < len(versionedHashes); pos += 32 {
		versionedHash := libcommon.BytesToHash(txBytes[pos : pos+32])
		versionedHashes[i] = versionedHash
		i++
	}

	return versionedHashes
}

func computeSigningRootEpoch(epoch uint64, domain []byte) (libcommon.Hash, error) {
	b := make([]byte, 32)
	binary.LittleEndian.PutUint64(b, epoch)
	return utils.Keccak256(b, domain), nil
}

// transitionSlot is called each time there is a new slot to process
func transitionSlot(s abstract.BeaconState) error {
	slot := s.Slot()
	previousStateRoot := s.PreviousStateRoot()
	var err error
	if previousStateRoot == (libcommon.Hash{}) {
		previousStateRoot, err = s.HashSSZ()
		if err != nil {
			return err
		}
	}

	beaconConfig := s.BeaconConfig()

	s.SetStateRootAt(int(slot%beaconConfig.SlotsPerHistoricalRoot), previousStateRoot)

	latestBlockHeader := s.LatestBlockHeader()
	if latestBlockHeader.Root == [32]byte{} {
		latestBlockHeader.Root = previousStateRoot
		s.SetLatestBlockHeader(&latestBlockHeader)
	}
	blockHeader := s.LatestBlockHeader()

	previousBlockRoot, err := (&blockHeader).HashSSZ()
	if err != nil {
		return err
	}
	s.SetBlockRootAt(int(slot%beaconConfig.SlotsPerHistoricalRoot), previousBlockRoot)
	return nil
}
