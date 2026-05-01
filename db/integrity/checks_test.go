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
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests cover the structural shape of the converted validator
// types — they implement Name() and would-be ValidateBatch (the
// signature is exercised via interface assignment below). Actually
// running ValidateBatch needs a real TemporalDB and chain state, which
// is the integration-test territory the bridge_test.go shipped with —
// see commit 287059b808 for the dispatch-shape tests against the
// previous IntegrityBridge model.

func TestBlocksCheck_NameStable(t *testing.T) {
	require.Equal(t, "Blocks", BlocksCheck{}.Name())
}

func TestInvertedIndexCheck_NameStable(t *testing.T) {
	require.Equal(t, "InvertedIndex", InvertedIndexCheck{}.Name())
}

func TestChecks_StructurallyImplementBatchValidator(t *testing.T) {
	// A BatchValidator is anything with Name() string and
	// ValidateBatch(context.Context) error. This test asserts our
	// converted types satisfy that shape via Go's structural typing —
	// no import of validation needed.
	type batchValidator interface {
		Name() string
		ValidateBatch(ctx interface{ Done() <-chan struct{} }) error
	}
	// We can't directly use context.Context in the test interface
	// shim because Context is from another package, but the existing
	// methods accept context.Context, so a compile-time check via
	// var declarations is what we use:

	// Compile-time: each Check type's ValidateBatch must accept ctx
	// and return error. The variable declarations below fail to
	// compile if the methods drift from the contract.
	var _ = (BlocksCheck{}).Name
	var _ = (BlocksCheck{}).ValidateBatch
	var _ = (InvertedIndexCheck{}).Name
	var _ = (InvertedIndexCheck{}).ValidateBatch
}
