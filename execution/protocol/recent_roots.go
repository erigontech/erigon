// Copyright 2026 The Erigon Authors
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

package protocol

import (
	"encoding/binary"
	"errors"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
)

const (
	RecentRootLength    = 8192
	recentRootTupleSize = 72
)

// RecentRootCode is the ordinary contract runtime required by the public pool:
// https://eips.ethereum.org/EIPS/eip-8272#bytecode.
var RecentRootCode = common.FromHex("0x346100ba57366040146100c05736604836066100ba5780156100ba5761048081116100ba574b60005b602081013560c01c828110156100ba5780830361200011156100ba577f8f42481679c8e6fefa040974b3c905e0ce3f2e464ba93acdb074a41181617efc60005260488260203760686000207fbdc897da2177d260ff5f4be5d4b2aad43f89c3347a305b584fa5a2546d053daa60005290611fff1660c01b60405260486000205414156100ba5760480182811061002857005b60006000fd5b33600052602060006020376034600c20807f8f42481679c8e6fefa040974b3c905e0ce3f2e464ba93acdb074a41181617efc6040524b606852606052602060206088376068604020817fbdc897da2177d260ff5f4be5d4b2aad43f89c3347a305b584fa5a2546d053daa60a852611fff4b1660d05260c852604860a8205500")

var (
	recentRootEntryDomain   = crypto.Keccak256Hash([]byte("RECENT_ROOT_ENTRY"))
	recentRootStorageDomain = crypto.Keccak256Hash([]byte("RECENT_ROOT_STORAGE"))
)

// RecentRootReference records a storage dependency and the referenced slot.
// It is usable only at later slots and expires at Slot + RecentRootLength;
// keeping Slot avoids storing an expiry that can overflow uint64.
type RecentRootReference struct {
	StorageKey common.Hash
	EntryHash  common.Hash
	Slot       uint64
}

func (r RecentRootReference) ValidAt(slot uint64) bool {
	return r.Slot < slot && slot-r.Slot < RecentRootLength
}

// FrameTransactionRecentRoots uses the validation prefix, so approval frames
// targeting the verifier address do not become recent-root dependencies.
func FrameTransactionRecentRoots(tx *types.FrameTransaction) ([]RecentRootReference, error) {
	prefix, err := frameValidationPrefix(tx)
	if err != nil {
		return nil, err
	}
	if prefix.recentRoot < 0 {
		return nil, nil
	}
	return ParseRecentRootReferences(tx.Frames[prefix.recentRoot].Data)
}

func validateRecentRootDataLength(size int) error {
	const maxReferences = 16
	if size < recentRootTupleSize || size%recentRootTupleSize != 0 || size > maxReferences*recentRootTupleSize {
		return errors.New("invalid recent root data length")
	}
	return nil
}

func ParseRecentRootReferences(data []byte) ([]RecentRootReference, error) {
	if err := validateRecentRootDataLength(len(data)); err != nil {
		return nil, err
	}
	refs := make([]RecentRootReference, 0, len(data)/recentRootTupleSize)
	var entryData [32 + recentRootTupleSize]byte
	var storageData [32 + 32 + 8]byte
	copy(entryData[:], recentRootEntryDomain[:])
	copy(storageData[:], recentRootStorageDomain[:])
	for len(data) != 0 {
		tuple := data[:recentRootTupleSize]
		slot := binary.BigEndian.Uint64(tuple[32:40])
		copy(entryData[32:], tuple)
		copy(storageData[32:64], tuple[:32])
		binary.BigEndian.PutUint64(storageData[64:], slot%RecentRootLength)
		ref := RecentRootReference{
			StorageKey: crypto.Keccak256Hash(storageData[:]),
			EntryHash:  crypto.Keccak256Hash(entryData[:]),
			Slot:       slot,
		}
		// Deduplicate dependency records only; the EVM still checks and charges every tuple.
		if !slices.Contains(refs, ref) {
			refs = append(refs, ref)
		}
		data = data[recentRootTupleSize:]
	}
	return refs, nil
}
