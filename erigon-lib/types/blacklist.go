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

package types

import "github.com/erigontech/erigon-lib/common"

// This is introduced because of the Tendermint IAVL Merkle Proof verification exploitation.
var NanoBlackList = []common.Address{
	common.HexToAddress("0x489A8756C18C0b8B24EC2a2b9FF3D4d447F79BEc"),
	common.HexToAddress("0xFd6042Df3D74ce9959922FeC559d7995F3933c55"),
	// Test Account
	common.HexToAddress("0xdb789Eb5BDb4E559beD199B8b82dED94e1d056C9"),
}
