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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
// General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package v3

import (
	"bytes"
	"fmt"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment"
)

func accountUpdate(value []byte, found bool, update *commitment.Update) (*commitment.Update, []byte, error) {
	result := &commitment.Update{CodeHash: empty.CodeHash}
	var storageRoot []byte
	if found {
		nonce, balance, codeHash, root, err := decodeAccountLeaf(value)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", errNodeRecord, err)
		}
		storageRoot = root
		result.Nonce = nonce
		result.Balance.Set(&balance)
		copy(result.CodeHash[:], codeHash)
	}
	if update == nil {
		return result, storageRoot, nil
	}
	if update.Flags&commitment.BalanceUpdate != 0 {
		result.Balance.Set(&update.Balance)
	}
	if update.Flags&commitment.NonceUpdate != 0 {
		result.Nonce = update.Nonce
	}
	if update.Flags&commitment.CodeUpdate != 0 {
		result.CodeHash = update.CodeHash
	}
	return result, storageRoot, nil
}

func accountLeafAt(n *node, path []byte) (value []byte, found, stored bool) {
	if child := rootExtensionChild(n); child != nil {
		n = child
	}
	if n == nil || len(path) != 64 || !bytes.HasPrefix(path, n.path) || len(n.path) >= len(path) {
		return nil, false, false
	}
	nib := int(path[len(n.path)])
	bit := uint16(1) << nib
	if n.childMask&bit == 0 {
		return nil, false, false
	}
	if n.leafMask&bit != 0 {
		suffix, value := n.leafAt(nib)
		if !packedMatches(suffix, path[len(n.path)+1:]) {
			return nil, false, false
		}
		return value, true, false
	}
	if child := n.child(nib); child != nil {
		return accountLeafAt(child, path)
	}
	return nil, false, n.hasChildHash(nib) && bytes.HasPrefix(path, n.childPath(nib, nil))
}
