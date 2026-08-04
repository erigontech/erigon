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

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
)

// The module is the one owner of the domain state cache: callers pass a byte
// budget, never a constructed cache, so a disabled cache cannot be built
// upstream and leak its memory-envelope reservation.
func TestNewDomainStateCacheRespectsUseStateCache(t *testing.T) {
	prev := dbg.UseStateCache
	t.Cleanup(func() { dbg.SetUseStateCache(prev) })

	dbg.SetUseStateCache(false)
	require.Nil(t, newDomainStateCache(0), "disabled mode must construct no cache")
	require.Nil(t, newDomainStateCache(16*datasize.MB), "a budget must not override the kill switch")

	dbg.SetUseStateCache(true)
	sc := newDomainStateCache(16 * datasize.MB)
	require.NotNil(t, sc)
	sc.Close()
	scDefault := newDomainStateCache(0)
	require.NotNil(t, scDefault, "zero budget means the production default, not no cache")
	scDefault.Close()
}
