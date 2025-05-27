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

package testhelpers

import (
	"github.com/erigontech/erigon/txnprovider/shutter"
)

func Signatures(signers []Keyper, data shutter.DecryptionKeysSignatureData) ([][]byte, error) {
	sigs := make([][]byte, len(signers))
	for i, signer := range signers {
		var err error
		sigs[i], err = data.Sign(signer.PrivateKey)
		if err != nil {
			return nil, err
		}
	}

	return sigs, nil
}
