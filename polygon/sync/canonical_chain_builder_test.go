package sync

import (
	"testing"

	"github.com/stretchr/testify/require"

	heimdallspan "github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/core/types"
)

type testDifficultyCalculator struct {
}

func (*testDifficultyCalculator) HeaderDifficulty(*types.Header) (uint64, error) {
	return 0, nil
}

func (*testDifficultyCalculator) SetSpan(*heimdallspan.HeimdallSpan) {}

func TestCanonicalChainBuilderConnectEmpty(t *testing.T) {
	difficultyCalc := testDifficultyCalculator{}
	builder := NewCanonicalChainBuilder(new(types.Header), &difficultyCalc)
	err := builder.Connect([]*types.Header{})
	require.Nil(t, err)
}
