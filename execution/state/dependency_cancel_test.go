package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Reading a slot that an unfinished contract creation cleared records the dependency and cancels the run.
func TestInFlightCreationCancelsAnOldSlotReader(t *testing.T) {
	addr, slot, reader, versions, _ := createdOverStorage(t)
	versions.InitSpecRuns(1)
	run := &countingCanceller{}
	versions.SetSpecRun(1, &SpecRun{Run: run})

	ibs := NewWithVersionMap(reader, versions)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)

	_, err := ibs.GetCommittedState(addr, slot)
	require.NoError(t, err)
	require.Equal(t, 0, ibs.DepTxIndex())
	require.Equal(t, 1, run.n, "reading a slot cleared by an unfinished creation cancels the run")
}
