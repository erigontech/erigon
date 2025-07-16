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

package types

import (
	"testing"

	"github.com/erigontech/erigon-lib/common/empty"
)

func TestEmptyRequestsHashCalculation(t *testing.T) {
	reqs := make(FlatRequests, 0)
	h := reqs.Hash()
	testH := empty.RequestsHash
	if *h != testH {
		t.Errorf("Requests Hash calculation error for empty hash, expected: %v, got: %v", testH, h)
	}

	reqs = make(FlatRequests, 3)
	h = reqs.Hash()
	if *h != testH {
		t.Errorf("Requests Hash calculation error for empty hash, expected: %v, got: %v", testH, h)
	}
}
