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

package execmodule

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state/execctx"
)

func newGenTestModule() *ExecModule {
	em := &ExecModule{}
	em.fgIdle = sync.NewCond(&em.fgMu)
	return em
}

func TestGenStackCountersAndLatestGen(t *testing.T) {
	em := newGenTestModule()
	a := &commitGen{sd: new(execctx.SharedDomains)}
	b := &commitGen{sd: new(execctx.SharedDomains)}

	em.addGen(a)
	em.addGen(b)
	require.Equal(t, 2, em.uncommittedGens)
	require.True(t, em.latestGen() == b.sd, "latestGen is the newest uncommitted generation")

	em.markGenCommitted(b)
	require.Equal(t, 1, em.uncommittedGens)
	require.Nil(t, em.latestGen(), "a committed newest generation means read straight from the DB")

	em.markGenCommitted(a)
	require.Equal(t, 0, em.uncommittedGens)
	em.WaitCommitsDrained() // returns immediately once the backlog is empty
}

func TestGenEpochGuardSkipsSupersededGen(t *testing.T) {
	em := newGenTestModule()
	a := &commitGen{sd: new(execctx.SharedDomains)}
	em.addGen(a)
	require.Equal(t, uint64(0), a.epoch)
	require.False(t, em.genSuperseded(a), "a freshly enqueued generation is current")

	// closeAllGens bumps genEpoch to discard the queued set (the SetHead-unwind
	// path); it also closes the real SharedDomains, so exercise just the bump here.
	em.fgMu.Lock()
	em.genEpoch++
	em.fgMu.Unlock()
	require.True(t, em.genSuperseded(a), "a generation whose set was discarded is superseded")

	c := &commitGen{sd: new(execctx.SharedDomains)}
	em.addGen(c)
	require.Equal(t, uint64(1), c.epoch)
	require.False(t, em.genSuperseded(c), "a generation enqueued after the bump is current")
}
