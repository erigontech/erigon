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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// A serial IBS (no versionMap, waitCommit nil) reports a usable stamp, so the vm
// frame slot cache is on; read tracing disables it, since tracing must observe
// every read.
func TestReadStamp_Gating(t *testing.T) {
	t.Parallel()
	ibs := New(&emptyReader{})
	defer ibs.Close()

	_, ok := ibs.ReadStamp()
	require.True(t, ok, "serial IBS must report a usable stamp")

	prev := dbg.TraceTransactionIO
	dbg.TraceTransactionIO = true
	t.Cleanup(func() { dbg.TraceTransactionIO = prev })
	_, ok = ibs.ReadStamp()
	require.False(t, ok, "read tracing must disable the slot cache")
}

// A dep-wait resume (a read that pauses on an in-flight estimate and re-reads the
// now-committed value) must change the stamp so the vm invalidates any slot it
// cached before the pause — the lower-txIndex write that landed during the pause
// is not journalled, so wakeGen is the only thing that moves the token here.
func TestReadStamp_ResumeChangesStamp(t *testing.T) {
	_, tx, domains := NewTestRwTx(t)
	mvhm := NewVersionMap(nil)
	ibs := NewWithVersionMap(NewReaderV3(domains.AsGetter(tx)), mvhm)
	defer ibs.Close()
	ibs.SetTxContext(1, 5)
	ibs.SetVersion(0)

	addr := accounts.InternAddress(common.HexToAddress("0xc0ffee"))
	key := accounts.InternKey(common.HexToHash("0x01"))
	// Lower-txIndex predecessor write is in flight (incomplete → ESTIMATE).
	mvhm.WriteStorage(addr, key, Version{TxIndex: 2}, uint256.Int{}, false)

	resumes := 0
	ibs.SetWaitCommit(func(int) bool {
		resumes++
		return resumes == 1 // pause once, then let the read fall through
	})

	before, ok := ibs.ReadStamp()
	require.True(t, ok)

	_, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	require.GreaterOrEqual(t, resumes, 1, "the read must have paused on the in-flight estimate")

	after, ok := ibs.ReadStamp()
	require.True(t, ok)
	// GetState does not journal, so wakeGen is the only field that can have moved.
	require.NotEqual(t, before, after, "a dep-wait resume must change the stamp (wakeGen) so the slot cache invalidates")
}
