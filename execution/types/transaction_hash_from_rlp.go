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

package types

import (
	"errors"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
)

var (
	errEmptyTxnRLP = errors.New("empty rlp encoded transaction")
	errShortTxnRLP = errors.New("short rlp encoded transaction")
)

// TxnHashFromRLP returns the hash of a transaction stored by
// rawdb.WriteTransactions, without decoding it.
//
// A transaction hashes its canonical (EIP-2718) form, which the stored RLP
// already contains verbatim: legacy transactions are stored as the canonical
// list itself, typed ones as that canonical form wrapped in an RLP string.
func TxnHashFromRLP(txnRlp []byte) (common.Hash, error) {
	if len(txnRlp) == 0 {
		return common.Hash{}, errEmptyTxnRLP
	}
	kind, content, rest, err := rlp.Split(txnRlp)
	if err != nil {
		return common.Hash{}, err
	}
	if len(rest) != 0 {
		return common.Hash{}, errTrailingBytes
	}
	// Reject what the decode this replaces would have rejected outright: an empty
	// legacy list, and a typed envelope of only a type byte. Hashing those would
	// index a hash of data that cannot be a transaction.
	switch kind {
	case rlp.List:
		if len(content) == 0 {
			return common.Hash{}, errEmptyTxnRLP
		}
		return crypto.Keccak256Hash(txnRlp), nil
	case rlp.String:
		if len(content) <= 1 {
			return common.Hash{}, errShortTxnRLP
		}
		return crypto.Keccak256Hash(content), nil
	case rlp.Byte:
		// A bare byte holds neither a legacy list nor a type prefix plus payload.
		return common.Hash{}, errShortTxnRLP
	default:
		return common.Hash{}, rlp.ErrExpectedString
	}
}
