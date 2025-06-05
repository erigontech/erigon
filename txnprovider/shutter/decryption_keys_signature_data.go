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

package shutter

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	merkletree "github.com/erigontech/erigon/cl/merkle_tree"
)

var (
	ErrTooManyIdentityPreimages      = errors.New("too many identity preimages")
	ErrIncorrectIdentityPreimageSize = errors.New("incorrect identity preimage size")
)

const (
	identityPreimageSize   = 52
	identityPreimagesLimit = 1024
)

type IdentityPreimage [identityPreimageSize]byte

func (ip *IdentityPreimage) EncodingSizeSSZ() int {
	return identityPreimageSize
}

func (ip *IdentityPreimage) EncodeSSZ(dst []byte) ([]byte, error) {
	return append(dst, ip[:]...), nil
}

func (ip *IdentityPreimage) DecodeSSZ(buf []byte, _ int) error {
	if len(buf) != identityPreimageSize {
		return fmt.Errorf("%w: len=%d", ErrIncorrectIdentityPreimageSize, len(ip))
	}

	copy(ip[:], buf)
	return nil
}

func (ip *IdentityPreimage) Clone() clonable.Clonable {
	clone := IdentityPreimage(slices.Clone(ip[:]))
	return &clone
}

func (ip *IdentityPreimage) HashSSZ() ([32]byte, error) {
	return merkletree.BytesRoot(ip[:])
}

func (ip *IdentityPreimage) String() string {
	return hexutil.Encode(ip[:])
}

func IdentityPreimageFromBytes(b []byte) (*IdentityPreimage, error) {
	var ip IdentityPreimage
	err := ip.DecodeSSZ(b, 0)
	return &ip, err
}

func IdentityPreimageFromSenderPrefix(prefix [32]byte, sender common.Address) *IdentityPreimage {
	var ip IdentityPreimage
	copy(ip[:len(prefix)], prefix[:])
	copy(ip[len(prefix):], sender.Bytes())
	return &ip
}

type IdentityPreimages []*IdentityPreimage

func (ips IdentityPreimages) ToListSSZ() *solid.ListSSZ[*IdentityPreimage] {
	return solid.NewStaticListSSZFromList(ips, identityPreimagesLimit, identityPreimageSize)
}

type DecryptionKeysSignatureData struct {
	InstanceId        uint64
	Eon               EonIndex
	Slot              uint64
	TxnPointer        uint64
	IdentityPreimages *solid.ListSSZ[*IdentityPreimage]
}

func (d DecryptionKeysSignatureData) HashSSZ() ([32]byte, error) {
	if err := d.Validate(); err != nil {
		return [32]byte{}, err
	}

	r, err := merkletree.HashTreeRoot(d.InstanceId, uint64(d.Eon), d.Slot, d.TxnPointer, d.IdentityPreimages)
	if err != nil {
		return [32]byte{}, fmt.Errorf("%w: slot=%d, eon=%d", err, d.Slot, d.Eon)
	}

	return r, nil
}

func (d DecryptionKeysSignatureData) Sign(key *ecdsa.PrivateKey) ([]byte, error) {
	h, err := d.HashSSZ()
	if err != nil {
		return nil, err
	}

	return crypto.Sign(h[:], key)
}

func (d DecryptionKeysSignatureData) Verify(signature []byte, address common.Address) (bool, error) {
	h, err := d.HashSSZ()
	if err != nil {
		return false, err
	}

	pubKey, err := crypto.SigToPub(h[:], signature)
	if err != nil {
		return false, err
	}

	return crypto.PubkeyToAddress(*pubKey) == address, nil
}

func (d DecryptionKeysSignatureData) Validate() error {
	if d.IdentityPreimages.Len() > identityPreimagesLimit {
		return fmt.Errorf("%w: len=%d", ErrTooManyIdentityPreimages, d.IdentityPreimages.Len())
	}

	return nil
}
