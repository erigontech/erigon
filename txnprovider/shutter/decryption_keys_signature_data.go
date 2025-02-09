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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/crypto"
	merkletree "github.com/erigontech/erigon/cl/merkle_tree"
)

var (
	ErrTooManyIdentityPreimages = errors.New("too many identity preimages")
	ErrIdentityPreimageTooBig   = errors.New("identity preimage too big")
)

type IdentityPreimage []byte

func (ip IdentityPreimage) String() string {
	return hexutility.Encode(ip)
}

type IdentityPreimages []IdentityPreimage

func (ips IdentityPreimages) Validate() error {
	if len(ips) > 1024 {
		return ErrTooManyIdentityPreimages
	}

	for i, ip := range ips {
		if len(ip) > 52 {
			return fmt.Errorf("%w: ips=%d, len=%d", ErrIdentityPreimageTooBig, i, len(ip))
		}
	}

	return nil
}

func (ips IdentityPreimages) HashSSZ() ([32]byte, error) {
	if err := ips.Validate(); err != nil {
		return [32]byte{}, err
	}

	schema := make([]interface{}, len(ips))
	for i, ip := range ips {
		schema[i] = []byte(ip)
	}

	return merkletree.HashTreeRoot(schema...)
}

type DecryptionKeysSignatureData struct {
	InstanceId        uint64
	Eon               EonIndex
	Slot              uint64
	TxnPointer        uint64
	IdentityPreimages IdentityPreimages
}

func (d DecryptionKeysSignatureData) HashSSZ() ([32]byte, error) {
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

func (d DecryptionKeysSignatureData) Verify(signature []byte, address libcommon.Address) (bool, error) {
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
