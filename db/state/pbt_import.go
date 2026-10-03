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

package state

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
)

func ForEachPBinArtifactLeaf(snapshot io.ReaderAt, snapshotSize int64, hashFn eip8297.HashFn, emit func(PBinLeaf) error) (common.Hash, error) {
	if snapshot == nil || emit == nil {
		return common.Hash{}, fmt.Errorf("pbt import: missing artifact input")
	}
	builder, err := eip8297.NewStreamRootBuilder(hashFn)
	if err != nil {
		return common.Hash{}, err
	}
	add := func(key []byte, value [eip8297.ValueLength]byte) error {
		if addErr := builder.Add(key, value[:]); addErr != nil {
			return addErr
		}
		return emit(PBinLeaf{Key: key, Value: value[:]})
	}
	meta, err := artifact.ReadSnapshotStreamAt(snapshot, snapshotSize, artifact.SnapshotStreamCallbacks{
		Header: func(header artifact.Header) error {
			codeSize := uint64(0)
			switch header.Kind {
			case 1:
				codeSize = PBinIntegerUint64(header.CodeSize)
			case 2:
				codeSize = eip8297.DelegationCodeLength
			}
			basic, encodeErr := eip8297.EncodeBasicData(PBinIntegerUint64(header.Nonce), new(uint256.Int).SetBytes(header.Balance), codeSize)
			if encodeErr != nil {
				return encodeErr
			}
			if addErr := add(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.BasicDataLeafKey), basic); addErr != nil {
				return addErr
			}
			switch header.Kind {
			case 0, 1:
				if addErr := add(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.CodeHashLeafKey), eip8297.CodeHashValue(header.CodeHash)); addErr != nil {
					return addErr
				}
			case 2:
				code := append(append([]byte(nil), eip8297.DelegationMarker[:]...), header.Target[:]...)
				if addErr := add(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.DelegationLeafKey), eip8297.EncodeDelegation(code)); addErr != nil {
					return addErr
				}
			}
			for _, slot := range header.Slots {
				if addErr := add(eip8297.TreeKey(eip8297.AccountZone, header.AddressHash[:], eip8297.HeaderStorageOffset+slot.Index), eip8297.EncodeStorageValue(slot.Value)); addErr != nil {
					return addErr
				}
			}
			return nil
		},
		Code: func(group artifact.Group) error {
			for _, entry := range group.Entries {
				if len(entry.Value) > eip8297.ValueLength {
					return fmt.Errorf("pbt import: code chunk has invalid width")
				}
				var value [eip8297.ValueLength]byte
				copy(value[eip8297.ValueLength-len(entry.Value):], entry.Value)
				if addErr := add(eip8297.TreeKey(eip8297.CodeZone, group.StemHash[:], entry.Index), value); addErr != nil {
					return addErr
				}
			}
			return nil
		},
		Storage: func(addressHash common.Hash, groups func(func(artifact.Group) error) error) error {
			return groups(func(group artifact.Group) error {
				position := append(append([]byte(nil), addressHash[:]...), group.StemHash[:]...)
				for _, entry := range group.Entries {
					key := eip8297.TreeKey(eip8297.StorageZone, position, entry.Index)
					value, decodeErr := eip8297.DecodeLeafValue(key, entry.Value)
					if decodeErr != nil {
						return decodeErr
					}
					if addErr := add(key, value); addErr != nil {
						return addErr
					}
				}
				return nil
			})
		},
	})
	if err != nil {
		return common.Hash{}, fmt.Errorf("pbt import: read artifact: %w", err)
	}
	root, err := builder.RootHash()
	if err != nil {
		return common.Hash{}, err
	}
	if root != meta.Root {
		return common.Hash{}, fmt.Errorf("pbt import: artifact root %s differs from streamed root %s", meta.Root.Hex(), root.Hex())
	}
	return root, nil
}

func PBinIntegerUint64(value []byte) uint64 {
	if len(value) > 8 {
		return ^uint64(0)
	}
	var raw [8]byte
	copy(raw[8-len(value):], value)
	return binary.BigEndian.Uint64(raw[:])
}
