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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/rlp"
)

const (
	accountHasBalance  byte = 1 << 0
	accountHasCodeHash byte = 1 << 1
	accountHasStorage  byte = 1 << 2
	accountKnownFlags       = accountHasBalance | accountHasCodeHash | accountHasStorage
)

var (
	errAccountLeafEmpty     = errors.New("commitment v4: empty account leaf")
	errAccountLeafFlags     = errors.New("commitment v4: invalid account leaf flags")
	errAccountLeafTruncated = errors.New("commitment v4: truncated account leaf")
	errAccountLeafTrailing  = errors.New("commitment v4: trailing account leaf data")
)

func encodeAccountLeaf(u *commitment.Update, storageRoot []byte, dst []byte) []byte {
	if u == nil {
		panic("commitment v4: nil account update")
	}
	if len(storageRoot) != 0 && len(storageRoot) != length.Hash {
		panic(fmt.Sprintf("commitment v4: invalid storage root length %d", len(storageRoot)))
	}

	flags := byte(0)
	if !u.Balance.IsZero() {
		flags |= accountHasBalance
	}
	codeHash := u.CodeHash
	if codeHash == (common.Hash{}) || codeHash == empty.CodeHash {
		codeHash = common.Hash{}
	} else {
		flags |= accountHasCodeHash
	}
	if !isEmptyStorageRoot(storageRoot) {
		flags |= accountHasStorage
	}

	dst = append(dst, flags)
	var nonceBuf [binary.MaxVarintLen64]byte
	nonceLen := binary.PutUvarint(nonceBuf[:], u.Nonce)
	dst = append(dst, nonceBuf[:nonceLen]...)
	if flags&accountHasBalance != 0 {
		balance := u.Balance.Bytes()
		dst = append(dst, byte(len(balance)))
		dst = append(dst, balance...)
	}
	if flags&accountHasCodeHash != 0 {
		dst = append(dst, codeHash[:]...)
	}
	if flags&accountHasStorage != 0 {
		dst = append(dst, storageRoot...)
	}
	return dst
}

func decodeAccountLeaf(b []byte) (nonce uint64, balance uint256.Int, codeHash []byte, storageRoot []byte, err error) {
	if len(b) == 0 {
		return 0, balance, nil, nil, errAccountLeafEmpty
	}
	flags := b[0]
	if flags&^accountKnownFlags != 0 {
		return 0, balance, nil, nil, fmt.Errorf("%w: 0x%02x", errAccountLeafFlags, flags)
	}
	pos := 1
	var consumed int
	nonce, consumed = binary.Uvarint(b[pos:])
	if consumed <= 0 {
		return 0, balance, nil, nil, fmt.Errorf("%w: nonce", errAccountLeafTruncated)
	}
	var nonceBuf [binary.MaxVarintLen64]byte
	if binary.PutUvarint(nonceBuf[:], nonce) != consumed {
		return 0, balance, nil, nil, fmt.Errorf("%w: non-canonical nonce", errAccountLeafFlags)
	}
	pos += consumed

	if flags&accountHasBalance != 0 {
		if pos >= len(b) {
			return 0, balance, nil, nil, errAccountLeafTruncated
		}
		balanceLen := int(b[pos])
		pos++
		if balanceLen == 0 || balanceLen > 32 || pos+balanceLen > len(b) {
			return 0, balance, nil, nil, fmt.Errorf("%w: balance", errAccountLeafFlags)
		}
		if b[pos] == 0 {
			return 0, balance, nil, nil, fmt.Errorf("%w: non-canonical balance", errAccountLeafFlags)
		}
		balance.SetBytes(b[pos : pos+balanceLen])
		pos += balanceLen
	}

	if flags&accountHasCodeHash != 0 {
		if pos+length.Hash > len(b) {
			return 0, balance, nil, nil, errAccountLeafTruncated
		}
		codeHash = b[pos : pos+length.Hash]
		if isEmptyCodeHash(codeHash) {
			return 0, balance, nil, nil, fmt.Errorf("%w: empty code hash is present", errAccountLeafFlags)
		}
		pos += length.Hash
	}

	if flags&accountHasStorage != 0 {
		if pos+length.Hash > len(b) {
			return 0, balance, nil, nil, errAccountLeafTruncated
		}
		storageRoot = b[pos : pos+length.Hash]
		if isEmptyStorageRoot(storageRoot) {
			return 0, balance, nil, nil, fmt.Errorf("%w: empty storage root is present", errAccountLeafFlags)
		}
		pos += length.Hash
	} else {
		storageRoot = empty.RootHash[:]
	}
	if pos != len(b) {
		return 0, balance, nil, nil, errAccountLeafTrailing
	}
	return nonce, balance, codeHash, storageRoot, nil
}

func accountConsensusRLP(nonce uint64, balance *uint256.Int, storageRoot, codeHash []byte, dst []byte) []byte {
	if balance == nil {
		balance = new(uint256.Int)
	}
	storageRoot = canonicalStorageRoot(storageRoot)
	codeHash = canonicalCodeHash(codeHash)
	balanceBytes := balance.Bytes()
	contentLen := rlp.U64Len(nonce) + rlp.StringLen(balanceBytes) + 1 + length.Hash + 1 + length.Hash
	start := len(dst)
	dst = append(dst, make([]byte, rlp.ListLen(contentLen))...)
	pos := start + rlp.EncodeListPrefixToBuf(contentLen, dst[start:])
	pos += rlp.EncodeU64ToBuf(nonce, dst[pos:])
	pos += rlp.EncodeStringToBuf(balanceBytes, dst[pos:])
	dst[pos] = 0xa0
	pos++
	copy(dst[pos:], storageRoot)
	pos += length.Hash
	dst[pos] = 0xa0
	pos++
	copy(dst[pos:], codeHash)
	pos += length.Hash
	return dst[:pos]
}

func canonicalStorageRoot(root []byte) []byte {
	if isEmptyStorageRoot(root) {
		return empty.RootHash[:]
	}
	if len(root) != length.Hash {
		panic(fmt.Sprintf("commitment v4: invalid storage root length %d", len(root)))
	}
	return root
}

func canonicalCodeHash(codeHash []byte) []byte {
	if len(codeHash) == 0 || isEmptyCodeHash(codeHash) {
		return empty.CodeHash[:]
	}
	if len(codeHash) != length.Hash {
		panic(fmt.Sprintf("commitment v4: invalid code hash length %d", len(codeHash)))
	}
	return codeHash
}

func isEmptyStorageRoot(root []byte) bool {
	return len(root) == 0 || len(root) == length.Hash && (bytes.Equal(root, empty.RootHash[:]) || bytes.Equal(root, make([]byte, length.Hash)))
}

func isEmptyCodeHash(codeHash []byte) bool {
	return len(codeHash) == length.Hash && (bytes.Equal(codeHash, empty.CodeHash[:]) || bytes.Equal(codeHash, make([]byte, length.Hash)))
}
