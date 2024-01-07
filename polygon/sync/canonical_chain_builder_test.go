package sync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core/types"
)

type testDifficultyCalculator struct {
}

func (*testDifficultyCalculator) HeaderDifficulty(*types.Header) (uint64, error) {
	return 0, nil
}

func TestCanonicalChainBuilderConnectEmpty(t *testing.T) {
	difficultyCalc := testDifficultyCalculator{}
	builder := NewCanonicalChainBuilder(new(types.Header), &difficultyCalc)
	err := builder.Connect([]*types.Header{})
	require.Nil(t, err)
}
