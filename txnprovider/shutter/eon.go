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
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
)

var ErrInvalidKeyperIndex = errors.New("invalid keyper index")

type EonIndex uint64

type Eon struct {
	Index           EonIndex
	ActivationBlock uint64
	Key             []byte
	Threshold       uint64
	Members         []common.Address
}

func (e Eon) KeyperAt(index uint64) (common.Address, error) {
	if index >= uint64(len(e.Members)) {
		return common.Address{}, fmt.Errorf("%w: %d >= %d", ErrInvalidKeyperIndex, index, len(e.Members))
	}

	return e.Members[index], nil
}

func (e Eon) PublicKey() (*crypto.EonPublicKey, error) {
	eonPublicKey := new(crypto.EonPublicKey)
	err := eonPublicKey.Unmarshal(e.Key)
	return eonPublicKey, err
}

func EonLess(a, b Eon) bool {
	return a.Index < b.Index
}

func EpochSecretKeyFromBytes(b []byte) (*crypto.EpochSecretKey, error) {
	epochSecretKey := new(crypto.EpochSecretKey)
	err := epochSecretKey.Unmarshal(b)
	return epochSecretKey, err
}
