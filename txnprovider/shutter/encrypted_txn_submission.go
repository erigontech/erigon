// Copyright 2025 The Erigon Authors
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

//go:build !abigen

package shutter

import (
	"math/big"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type TxnIndex uint64

// EncryptedTxnSubmission mimics contracts.SequencerTransactionSubmitted but without the "Raw" attribute to save memory.
type EncryptedTxnSubmission struct {
	EonIndex             EonIndex
	TxnIndex             TxnIndex
	IdentityPrefix       [32]byte
	Sender               libcommon.Address
	EncryptedTransaction []byte
	GasLimit             *big.Int
	BlockNum             uint64
}

func (ets EncryptedTxnSubmission) IdentityPreimageBytes() []byte {
	buf := make([]byte, len(ets.IdentityPrefix)+len(ets.Sender))
	copy(buf[:len(ets.IdentityPrefix)], ets.IdentityPrefix[:])
	copy(buf[len(ets.IdentityPrefix):], ets.Sender.Bytes())
	return buf
}

func EncryptedTxnSubmissionFromLogEvent(event *contracts.SequencerTransactionSubmitted) EncryptedTxnSubmission {
	return EncryptedTxnSubmission{
		EonIndex:             EonIndex(event.Eon),
		TxnIndex:             TxnIndex(event.TxIndex),
		IdentityPrefix:       event.IdentityPrefix,
		Sender:               event.Sender,
		EncryptedTransaction: event.EncryptedTransaction,
		GasLimit:             event.GasLimit,
		BlockNum:             event.Raw.BlockNumber,
	}
}

func EncryptedTxnSubmissionLess(a, b EncryptedTxnSubmission) bool {
	if a.EonIndex < b.EonIndex {
		return true
	}

	if a.EonIndex == b.EonIndex && a.TxnIndex <= b.TxnIndex {
		return true
	}

	return false
}

func EncryptedTxnSubmissionsAreConsecutive(a, b EncryptedTxnSubmission) bool {
	return (a.EonIndex == b.EonIndex && a.TxnIndex+1 == b.TxnIndex) ||
		(a.EonIndex+1 == b.EonIndex && b.TxnIndex == 0)
}
