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
	"io"

	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
)

// FrameTxWrapper carries blob sidecars for pool and network transport. Hashing
// and execution use the embedded transaction; sidecars are outside its signed payload.
type FrameTxWrapper struct {
	*FrameTransaction
	WrapperVersion byte
	Blobs          Blobs
	Commitments    BlobKzgs
	Proofs         KZGProofs
}

type frameTxWrapperRLP struct {
	Transaction *frameTransactionRLP
	Version     byte
	Blobs       Blobs
	Commitments BlobKzgs
	Proofs      KZGProofs
}

func (tx *FrameTxWrapper) Unwrap() Transaction { return tx.FrameTransaction }

func (tx *FrameTxWrapper) DecodeRLP(s *rlp.Stream) error {
	var wrapped frameTxWrapperRLP
	if err := s.Decode(&wrapped); err != nil {
		return err
	}
	tx.FrameTransaction = (*FrameTransaction)(wrapped.Transaction)
	tx.WrapperVersion, tx.Blobs, tx.Commitments, tx.Proofs = wrapped.Version, wrapped.Blobs, wrapped.Commitments, wrapped.Proofs
	if err := tx.validateSidecarShape(); err != nil {
		return err
	}
	return tx.validateNonceKeys()
}

func (tx *FrameTxWrapper) MarshalBinaryWrapped(w io.Writer) error {
	if err := tx.validateSidecarShape(); err != nil {
		return err
	}
	if _, err := w.Write([]byte{FrameTxType}); err != nil {
		return err
	}
	return rlp.Encode(w, frameTxWrapperRLP{(*frameTransactionRLP)(tx.FrameTransaction), tx.WrapperVersion, tx.Blobs, tx.Commitments, tx.Proofs})
}

func (tx *FrameTxWrapper) validateSidecarShape() error {
	if tx.FrameTransaction == nil || tx.WrapperVersion != 1 {
		return errors.New("frame blobs require wrapper version 1")
	}
	n := len(tx.BlobVersionedHashes)
	if n == 0 || n != len(tx.Blobs) || n != len(tx.Commitments) || n*int(params.CellsPerExtBlob) != len(tx.Proofs) {
		return errors.New("invalid frame blob sidecar lengths")
	}
	return nil
}
