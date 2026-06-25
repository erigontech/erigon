package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/config3"
)

func TestRefsInCommitmentBranchesResolver(t *testing.T) {
	t.Parallel()

	var unset ErigonDBSettings
	require.Equal(t, config3.DefaultReferencesInCommitmentBranches, unset.RefsInCommitmentBranches(),
		"omitted references_in_commitment_branches must resolve to the default")
	require.False(t, unset.RefsInCommitmentBranches(), "default on performance is plain (false)")

	tr := true
	require.True(t, (&ErigonDBSettings{ReferencesInCommitmentBranches: &tr}).RefsInCommitmentBranches())

	fl := false
	require.False(t, (&ErigonDBSettings{ReferencesInCommitmentBranches: &fl}).RefsInCommitmentBranches())
}
