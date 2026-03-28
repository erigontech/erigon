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

package validate

import (
	"github.com/erigontech/erigon/common"
)

// CodeReader retrieves deployed contract bytecode for static validation.
//
// Implementations must be safe for concurrent use from pool validation goroutines.
// The caller should treat a nil-length return as "account has no code" and act
// accordingly (e.g. reject a transaction that requires a deployed sender).
//
// This is a domain interface: it does NOT live on the engine backend (ethBackend).
// The concrete implementation is provided by the chain/state layer and injected
// into the pool at construction time.
type CodeReader interface {
	// Code returns the deployed bytecode at addr at the current chain tip.
	// Returns (nil, nil) if the account exists but has no code.
	Code(addr common.Address) ([]byte, error)
}
