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

package integrity

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
)

// InvertedIndexCheck wraps E3EfFiles as a self-contained type
// implementing the BatchValidator contract (Name() / ValidateBatch()).
// Replaces the IntegrityBridge dispatch entry for the InvertedIndex
// check.
//
// Per the extension-point design (see
// node/components/storage/validation/EXTENDING.md), validators are
// first-class types that consumers compose into a Chain by listing.
type InvertedIndexCheck struct {
	DB       kv.TemporalRwDB
	FromStep uint64
	FailFast bool
}

// Name returns the stable identifier used in error wrapping and log
// output.
func (InvertedIndexCheck) Name() string { return "InvertedIndex" }

// ValidateBatch invokes E3EfFiles with the configured dependencies.
func (c InvertedIndexCheck) ValidateBatch(ctx context.Context) error {
	return E3EfFiles(ctx, c.DB, c.FailFast, c.FromStep)
}
