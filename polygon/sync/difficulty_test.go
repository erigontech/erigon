package sync

import (
	"testing"

	"github.com/ledgerwatch/log/v3"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/consensus/bor/borcfg"
	heimdallspan "github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/core/types"
)

func TestHeaderDifficultyNoSignature(t *testing.T) {
	borConfig := borcfg.BorConfig{}
	span := heimdallspan.HeimdallSpan{}
	logger := log.New()
	calc := NewDifficultyCalculator(&borConfig, &span, logger)
	_, err := calc.HeaderDifficulty(new(types.Header))
	require.ErrorContains(t, err, "signature suffix missing")
}
