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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A background build and merge can overlap; editing must stay disabled until
// every lock is released, or Preset* could mutate worker config mid-merge.
func TestWorkersCfgEditingLockIsReentrant(t *testing.T) {
	t.Parallel()
	w := &workersCfg{merge: 1, collateAndBuild: 1}

	editable := func() bool {
		ran := false
		w.trySet(func() { ran = true })
		return ran
	}

	require.True(t, editable(), "editing enabled initially")

	w.lockEditing()
	require.False(t, editable(), "disabled while one op holds the lock")

	w.lockEditing()
	w.unlockEditing()
	require.False(t, editable(), "still disabled while a second op holds it")

	w.unlockEditing()
	require.True(t, editable(), "re-enabled after the last lock is released")

	w.unlockEditing()
	require.True(t, editable(), "extra unlock must not underflow and disable editing")
}
