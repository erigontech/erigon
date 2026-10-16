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
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/rlp"
)

// SystemTxType is the EIP-2718 type of EEZ's unsigned system transactions.
const SystemTxType byte = 0x76

// SystemTxGasLimit is the execution budget of every system transaction; the
// envelope carries no gas fields.
const SystemTxGasLimit uint64 = 2_000_000

var (
	// SystemAddress is the implicit sender of system transactions.
	SystemAddress = common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee0076")
	// EEZL2Address is the EEZL2 predeploy, the only target of system
	// transactions.
	EEZL2Address = common.HexToAddress("0x4200000000000000000000000000000000000007")
)

// ErrSystemTx reports bytes that are not a valid type-0x76 transaction.
var ErrSystemTx = errors.New("invalid system transaction")

// SystemTx is the body of a type-0x76 transaction,
// rlp([chainId, nonce, to, value, input]). It has no signature: derivation
// authorizes it and every node rebuilds the same bytes.
type SystemTx struct {
	ChainID uint64
	Nonce   uint64
	To      common.Address
	Value   uint256.Int
	Input   []byte
}

// MarshalBinary returns the EIP-2718 encoding 0x76 ‖ rlp(body).
func (tx *SystemTx) MarshalBinary() ([]byte, error) {
	if tx.To != EEZL2Address {
		return nil, fmt.Errorf("%w: target %v is not EEZL2", ErrSystemTx, tx.To)
	}
	body, err := rlp.EncodeToBytes(tx)
	if err != nil {
		return nil, err
	}
	return append([]byte{SystemTxType}, body...), nil
}

// Hash returns the transaction hash, keccak256 of the EIP-2718 encoding.
func (tx *SystemTx) Hash() (common.Hash, error) {
	enc, err := tx.MarshalBinary()
	if err != nil {
		return common.Hash{}, err
	}
	return crypto.Keccak256Hash(enc), nil
}

// DecodeSystemTx decodes a type-0x76 transaction: canonical RLP of exactly
// the five body fields, targeting EEZL2, with nothing after it.
func DecodeSystemTx(enc []byte) (*SystemTx, error) {
	if len(enc) == 0 || enc[0] != SystemTxType {
		return nil, fmt.Errorf("%w: not type 0x76", ErrSystemTx)
	}
	var tx SystemTx
	if err := rlp.DecodeBytes(enc[1:], &tx); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSystemTx, err)
	}
	if tx.To != EEZL2Address {
		return nil, fmt.Errorf("%w: target %v is not EEZL2", ErrSystemTx, tx.To)
	}
	return &tx, nil
}
