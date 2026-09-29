package blockreplay_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/blockreplay"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
)

func TestReplayMainnetBlock(t *testing.T) {
	fx := loadFixture(t, "25604144")
	engine := merge.New(ethash.NewFaker())
	defer engine.Close()
	res, err := blockreplay.Replay(fx, chainspec.Mainnet.Config, engine, 0, log.New())
	require.NoError(t, err)
	require.NotNil(t, res)
}
