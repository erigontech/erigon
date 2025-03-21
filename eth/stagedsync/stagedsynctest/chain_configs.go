package stagedsynctest

import (
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

func BorDevnetChainConfigWithNoBlockSealDelays() *chain.Config {
	// take care not to mutate global var (shallow copy)
	chainConfigCopy := *params.BorDevnetChainConfig
	borConfigCopy := *chainConfigCopy.Bor.(*borcfg.BorConfig)
	borConfigCopy.Period = map[string]uint64{
		"0": 0,
	}
	borConfigCopy.ProducerDelay = map[string]uint64{
		"0": 0,
	}
	chainConfigCopy.Bor = &borConfigCopy
	return &chainConfigCopy
}
