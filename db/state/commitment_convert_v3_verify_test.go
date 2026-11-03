package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyCommitmentV3CheckRejectsOrphans(t *testing.T) {
	err := verifyCommitmentV3Check(CommitmentV3Check{Orphans: 1, BlockNum: 2, Records: 3})
	require.ErrorContains(t, err, "orphan records")
}
