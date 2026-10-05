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

package commitment

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
)

type FeedSlot struct {
	Hash  [32]byte
	Value []byte
}

type FeedAccount struct {
	Hash   [32]byte
	Update *Update
	Slots  []FeedSlot
}

type Feed struct {
	Accounts []FeedAccount
	Keys     int
}

type PBinFeed struct {
	Accounts []PBinFeedAccount
}

type PBinCodeStats struct {
	CodeBearingAccounts uint64
	UniqueCodeHashes    uint64
}

type PBinFeedAccount struct {
	Address     []byte
	Exists      bool
	Nonce       uint64
	Balance     uint256.Int
	CodeHash    common.Hash
	Wiped       bool
	CodeWritten bool
	Code        []byte
	Slots       []PBinFeedSlot
}

type PBinFeedSlot struct {
	Key   []byte
	Value []byte
}
