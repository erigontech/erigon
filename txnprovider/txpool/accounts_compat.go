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

package txpool

import (
	"github.com/erigontech/erigon/common"
	accpkg "github.com/erigontech/erigon/execution/types/accounts"
)

// accounts is a package-level accessor providing account-related constants for this package.
// Tests compiled in package txpool (e.g. pool_test.go) can reference accounts.EmptyCodeHash
// without needing to import the accounts package separately.
//
// Note: files that import "github.com/erigontech/erigon/execution/types/accounts" directly
// (e.g. senders.go) use their file-scoped import name, which shadows this variable within
// those files — this is standard Go scoping behaviour and causes no compile errors.
var accounts = struct { //nolint:gochecknoglobals
	EmptyCodeHash common.Hash
}{
	EmptyCodeHash: accpkg.EmptyCodeHash,
}
