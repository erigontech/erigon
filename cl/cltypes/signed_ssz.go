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

package cltypes

import (
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/ssz"
)

// SSZ quartet shared by the signed container types ({Message, Signature} pairs).

func encodeSigned(buf []byte, msg ssz2.SizedObjectSSZ, sig []byte) ([]byte, error) {
	return ssz2.MarshalSSZ(buf, msg, sig)
}

func decodeSigned(buf []byte, version int, msg ssz2.SizedObjectSSZ, sig []byte) error {
	return ssz2.UnmarshalSSZ(buf, version, msg, sig)
}

func sizeSigned(msg ssz2.SizedObjectSSZ) int {
	size := length.Bytes96 + msg.EncodingSizeSSZ()
	if !msg.Static() {
		size += 4
	}
	return size
}

func hashSigned(msg ssz.HashableSSZ, sig []byte) ([32]byte, error) {
	return merkle_tree.HashTreeRoot(msg, sig)
}
