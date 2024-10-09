package utils

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

const VERSIONED_HASH_VERSION_KZG byte = byte(1)

func KzgCommitmentToVersionedHash(kzgCommitment libcommon.Bytes48) (libcommon.Hash, error) {
	versionedHash := [32]byte{}
	kzgCommitmentHash := Sha256(kzgCommitment[:])

	buf := append([]byte{}, VERSIONED_HASH_VERSION_KZG)
	buf = append(buf, kzgCommitmentHash[1:]...)
	copy(versionedHash[:], buf)

	return versionedHash, nil
}
