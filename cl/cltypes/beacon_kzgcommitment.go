// Copyright 2024 The Erigon Authors
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
	"encoding/json"
	"reflect"

	gokzg4844 "github.com/crate-crypto/go-kzg-4844"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon/cl/merkle_tree"
	ssz2 "github.com/ledgerwatch/erigon/cl/ssz"
)

var (
	blobT = reflect.TypeOf(Blob{})
)

type Blob gokzg4844.Blob
type KZGProof gokzg4844.KZGProof // [48]byte

// https://github.com/ethereum/consensus-specs/blob/3a2304981a3b820a22b518fe4859f4bba0ebc83b/specs/deneb/polynomial-commitments.md#custom-types
const BYTES_PER_FIELD_ELEMENT = 32
const FIELD_ELEMENTS_PER_BLOB = 4096
const BYTES_PER_BLOB = uint64(BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_BLOB)

type KZGCommitment gokzg4844.KZGCommitment

func (b KZGCommitment) MarshalJSON() ([]byte, error) {
	return json.Marshal(libcommon.Bytes48(b))
}

func (b *KZGCommitment) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, (*libcommon.Bytes48)(b))
}

func (b *KZGCommitment) Copy() *KZGCommitment {
	copy := *b
	return &copy
}

func (b *KZGCommitment) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, b[:]...), nil
}

func (b *KZGCommitment) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b[:])
}

func (b *KZGCommitment) EncodingSizeSSZ() int {
	return 48
}

func (b *KZGCommitment) HashSSZ() ([32]byte, error) {
	return merkle_tree.BytesRoot(b[:])
}

func (b *Blob) MarshalJSON() ([]byte, error) {
	return json.Marshal(hexutility.Bytes(b[:]))
}

func (b *Blob) UnmarshalJSON(in []byte) error {
	return hexutility.UnmarshalFixedJSON(blobT, in, b[:])
}

func (b *Blob) Clone() clonable.Clonable {
	return &Blob{}
}

func (b *Blob) DecodeSSZ(buf []byte, version int) error {
	copy(b[:], buf)
	return nil
}

func (b *Blob) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, b[:]...), nil
}

func (b *Blob) EncodingSizeSSZ() int {
	return len(b[:])
}

func (b *Blob) HashSSZ() ([32]byte, error) {
	return merkle_tree.BytesRoot(b[:])
}
