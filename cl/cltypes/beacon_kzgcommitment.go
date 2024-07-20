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
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/merkle_tree"
	ssz2 "github.com/erigontech/erigon/cl/ssz"
)

var (
	blobT = reflect.TypeOf(Blob{})

	_ ssz2.SizedObjectSSZ = (*Blob)(nil)
	_ ssz2.SizedObjectSSZ = (*KZGProof)(nil)
)

type Blob gokzg4844.Blob
type KZGProof gokzg4844.KZGProof // [48]byte

const (
	// https://github.com/ethereum/consensus-specs/blob/3a2304981a3b820a22b518fe4859f4bba0ebc83b/specs/deneb/polynomial-commitments.md#custom-types
	BYTES_PER_FIELD_ELEMENT = 32
	FIELD_ELEMENTS_PER_BLOB = 4096
	BYTES_KZG_PROOF         = 48
	BYTES_PER_BLOB          = BYTES_PER_FIELD_ELEMENT * FIELD_ELEMENTS_PER_BLOB
)

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

func (b *Blob) Static() bool {
	return true
}

func (b *Blob) HashSSZ() ([32]byte, error) {
	return merkle_tree.BytesRoot(b[:])
}

func (b *KZGProof) MarshalJSON() ([]byte, error) {
	return json.Marshal(libcommon.Bytes48(*b))
}

func (b *KZGProof) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, (*libcommon.Bytes48)(b))
}

func (b *KZGProof) DecodeSSZ(buf []byte, version int) error {
	return ssz2.UnmarshalSSZ(buf, version, b[:])
}

func (b *KZGProof) EncodeSSZ(buf []byte) ([]byte, error) {
	return append(buf, b[:]...), nil
}

func (b *KZGProof) EncodingSizeSSZ() int {
	return 48
}

func (b *KZGProof) HashSSZ() ([32]byte, error) {
	return merkle_tree.BytesRoot(b[:])
}

func (b *KZGProof) Clone() clonable.Clonable {
	return &KZGProof{}
}

func (b *KZGProof) Static() bool {
	return true
}
