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

package l1_test

import (
	"testing"

	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
)

// TestNativeConformance is T-L1-1 for the native reader over an in-process
// Erigon L1.
func TestNativeConformance(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	l1test.Conformance(t, l1.NewNative(m.DB, m.BlockReader, m.ReceiptsReader, m.ChainConfig), model, l1test.SampleFilters)
}
