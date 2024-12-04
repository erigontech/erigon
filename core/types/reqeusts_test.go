// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"crypto/sha256"
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
)

func TestEmptyRequestsHashCalculation(t *testing.T) {
	reqs := make(FlatRequests, 0)
	h := reqs.Hash()
	testHArr := sha256.Sum256([]byte(""))
	testH := libcommon.BytesToHash(testHArr[:])
	if *h != testH {
		t.Errorf("Requests Hash calculation error for empty hash, expected: %v, got: %v", testH, h)
	}
}
