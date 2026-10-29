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

package execmodule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestIsValidatedDoesNotWaitForValidation(t *testing.T) {
	fv := newForkValidator(t.Context(), 0, nil, nil, 1, nil)
	hash := common.Hash{1}
	fv.validHashes.Add(hash, true)

	fv.lock.Lock()
	defer fv.lock.Unlock()
	validated := make(chan bool, 1)
	go func() { validated <- fv.IsValidated(hash) }()
	select {
	case ok := <-validated:
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("IsValidated waited for the fork validator lock")
	}
}
