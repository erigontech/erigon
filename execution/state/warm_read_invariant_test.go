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

package state

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// The assert is checked directly rather than through warmReadable so the test
// does not depend on the process-wide dbg.AssertEnabled flag.
func TestAssertNoDeletedResident(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x01})
	other := accounts.InternAddress([20]byte{0x02})

	objects := map[accounts.Address]*stateObject{}
	assert.NotPanics(t, func() { assertNoDeletedResident(objects, addr) },
		"no resident object: nothing to serve stale")

	objects[addr] = &stateObject{}
	assert.NotPanics(t, func() { assertNoDeletedResident(objects, addr) },
		"live resident object is fine")

	objects[addr] = &stateObject{deleted: true}
	assert.Panics(t, func() { assertNoDeletedResident(objects, addr) },
		"deleted resident object must trip the assert")

	assert.NotPanics(t, func() { assertNoDeletedResident(objects, other) },
		"a different address is unaffected")
}
