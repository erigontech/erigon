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

package utils

import (
	libcommon "github.com/erigontech/erigon-lib/common"
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
