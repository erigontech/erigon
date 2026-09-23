package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
)

func TestShouldWaitForReadAhead(t *testing.T) {
	oldReadAheadWait := dbg.ReadAheadWait
	t.Cleanup(func() { dbg.ReadAheadWait = oldReadAheadWait })
	dbg.ReadAheadWait = false
	require.False(t, shouldWaitForReadAhead(false))
	require.False(t, shouldWaitForReadAhead(true))
	dbg.ReadAheadWait = true
	require.False(t, shouldWaitForReadAhead(false))
	require.True(t, shouldWaitForReadAhead(true))
}
