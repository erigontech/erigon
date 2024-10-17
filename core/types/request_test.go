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

import (
	// "bytes"
	"testing"
	// "github.com/erigontech/erigon/cl/utils"
)

func TestFlatRequestRoot(t *testing.T) {
	r := make(FlatRequests, 3)
	// r = append(r, &FlatRequest{})
	// bt3 := make([]byte, 3)
	t.Error(r.Hash())
	// t.Error(r.Hash3())
	// t.Error(rlpHash([]byte{}))
	// t.Error(utils.Sha256(nil))
}
