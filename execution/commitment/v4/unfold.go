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

package v4

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/execution/commitment"
)

var (
	ErrUnfoldPlane    = errors.New("commitment v4: invalid unfold plane")
	ErrUnfoldAddress  = errors.New("commitment v4: invalid storage address hash")
	ErrUnfoldEmbedded = errors.New("commitment v4: embedded children are unsupported")
)

func unfold(ctx commitment.PatriciaContext, path []byte, plane byte, addrHash []byte) (*node, error) {
	if ctx == nil {
		return nil, errors.New("commitment v4: nil unfold context")
	}
	if plane != planeAccount && plane != planeStorage {
		return nil, fmt.Errorf("%w: 0x%02x", ErrUnfoldPlane, plane)
	}
	if len(path) > 63 {
		return nil, fmt.Errorf("commitment v4: path depth %d", len(path))
	}
	for _, nib := range path {
		if nib > 0x0f {
			return nil, fmt.Errorf("commitment v4: invalid path nibble %d", nib)
		}
	}

	var key []byte
	if plane == planeAccount {
		if len(addrHash) != 0 {
			return nil, ErrUnfoldAddress
		}
		key = AccountNodeKey(path, nil)
	} else {
		if len(addrHash) != 32 {
			return nil, ErrUnfoldAddress
		}
		var address [32]byte
		copy(address[:], addrHash)
		key = StorageNodeKey(address, path, nil)
	}

	data, _, err := ctx.Branch(key)
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, nil
	}
	if len(data) == 0 {
		return fork(path), nil
	}
	if err := Validate(data, len(path)); err != nil {
		return nil, err
	}

	record := NewRecord(data, len(path))
	n := fork(path)
	n.plane = plane
	n.storageRoot = plane == planeStorage && len(path) == 0
	if len(path) == 0 && data[0]&hdrHasSelfExt != 0 {
		n.path = unpackPath(record.SelfExt()[1:], int(data[1]), nil)
	}
	if record.EmbMask() != 0 {
		return nil, ErrUnfoldEmbedded
	}
	if record.isLeafRoot() {
		hashedKey, value := record.LeafRootBody()
		fullPath := unpackPath(hashedKey, 64, nil)
		n.setLeaf(int(fullPath[0]), packPath(fullPath[1:], nil), value)
		return n, nil
	}

	for nib := range 16 {
		bit := uint16(1) << nib
		if record.ChildMask()&bit == 0 {
			continue
		}
		if record.LeafMask()&bit != 0 {
			suffix, value := record.LeafAt(nib)
			n.setLeaf(nib, suffix, value)
			continue
		}
		hash := record.SlotAt(nib)
		if len(hash) != 32 {
			return nil, fmt.Errorf("%w: child %d hash", ErrInvalidRecord, nib)
		}
		ext, err := decodeExtension(record.ExtAt(nib))
		if err != nil {
			return nil, err
		}
		n.setStoredChild(nib, hash, ext)
	}
	return n, nil
}

func decodeExtension(encoded []byte) ([]byte, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	extLen := int(encoded[0])
	need := 1 + packedLen(extLen)
	if len(encoded) != need {
		return nil, ErrRecordTrailer
	}
	return unpackPath(encoded[1:], extLen, nil), nil
}
