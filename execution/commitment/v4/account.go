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
	accountHasNonce    byte = 1 << 0
	accountHasCodeHash byte = 1 << 1
	accountHasStorage  byte = 1 << 2
	accountKnownFlags       = accountHasNonce | accountHasCodeHash | accountHasStorage
)

var (
	errAccountLeafEmpty     = errors.New("commitment v4: empty account leaf")
	errAccountLeafFlags     = errors.New("commitment v4: invalid account leaf flags")
	errAccountLeafTruncated = errors.New("commitment v4: truncated account leaf")
)

// widest account leaf body: flags + uvarint nonce + code hash + storage root + balance
const accountLeafScratch = 1 + binary.MaxVarintLen64 + 3*length.Hash

func encodeAccountLeaf(u *commitment.Update, storageRoot []byte, dst []byte) []byte {
	if u == nil {
		panic("commitment v4: nil account update")
	}
	if len(storageRoot) != 0 && len(storageRoot) != length.Hash {
		panic(fmt.Sprintf("commitment v4: invalid storage root length %d", len(storageRoot)))
	}

	flags := byte(0)
	if u.Nonce != 0 {
		flags |= accountHasNonce
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
	if flags&accountHasNonce != 0 {
		var nonceBuf [binary.MaxVarintLen64]byte
		dst = append(dst, nonceBuf[:binary.PutUvarint(nonceBuf[:], u.Nonce)]...)
	}
	if flags&accountHasCodeHash != 0 {
		dst = append(dst, codeHash[:]...)
	}
	if flags&accountHasStorage != 0 {
		dst = append(dst, storageRoot...)
	}
	if balanceLen := u.Balance.ByteLen(); balanceLen != 0 {
		at := len(dst)
		dst = append(dst, make([]byte, balanceLen)...)
		u.Balance.WriteToSlice(dst[at:])
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

	if flags&accountHasNonce != 0 {
		var consumed int
		nonce, consumed = binary.Uvarint(b[pos:])
		if consumed <= 0 {
			return 0, balance, nil, nil, fmt.Errorf("%w: nonce", errAccountLeafTruncated)
		}
		var nonceBuf [binary.MaxVarintLen64]byte
		if nonce == 0 || binary.PutUvarint(nonceBuf[:], nonce) != consumed {
			return 0, balance, nil, nil, fmt.Errorf("%w: non-canonical nonce", errAccountLeafFlags)
		}
		pos += consumed
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

	if rest := b[pos:]; len(rest) != 0 {
		if len(rest) > length.Hash || rest[0] == 0 {
			return 0, balance, nil, nil, fmt.Errorf("%w: balance", errAccountLeafFlags)
		}
		balance.SetBytes(rest)
	}
	return nonce, balance, codeHash, storageRoot, nil
}

func accountConsensusRLP(nonce uint64, balance *uint256.Int, storageRoot, codeHash []byte, dst []byte) []byte {
	if balance == nil {
		balance = new(uint256.Int)
	}
	storageRoot = canonicalStorageRoot(storageRoot)
	codeHash = canonicalCodeHash(codeHash)
	var balanceBuf [length.Hash]byte
	balanceBytes := balanceBuf[:balance.ByteLen()]
	balance.WriteToSlice(balanceBytes)
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
	if len(root) == 0 {
		return true
	}
	if len(root) != length.Hash {
		return false
	}
	h := (*common.Hash)(root)
	return *h == empty.RootHash || *h == (common.Hash{})
}

func isEmptyCodeHash(codeHash []byte) bool {
	if len(codeHash) != length.Hash {
		return false
	}
	h := (*common.Hash)(codeHash)
	return *h == empty.CodeHash || *h == (common.Hash{})
}
