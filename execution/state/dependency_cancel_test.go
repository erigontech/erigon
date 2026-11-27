package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Reading a slot that an unfinished contract creation cleared records the dependency and cancels the run.
func TestInFlightCreationCancelsAnOldSlotReader(t *testing.T) {
	addr, slot, reader, versions, _ := createdOverStorage(t)
	ibs := NewWithVersionMap(reader, versions)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	var cancels int
	ibs.SetCancel(func() { cancels++ })

	_, err := ibs.GetCommittedState(addr, slot)
	require.NoError(t, err)
	require.Equal(t, 0, ibs.DepTxIndex())
	require.Equal(t, 1, cancels, "reading a slot cleared by an unfinished creation cancels the run")
}
