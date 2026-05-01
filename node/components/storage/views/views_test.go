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

package views_test

import (
	"testing"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/views"
)

// TestInventorySatisfiesAwaiter is a compile-time check that
// *snapshot.Inventory structurally satisfies views.Awaiter. The test
// fails the build (not a runtime check) if WaitForReady's signature
// drifts on either side.
func TestInventorySatisfiesAwaiter(t *testing.T) {
	var _ views.Awaiter = (*snapshot.Inventory)(nil)
}
