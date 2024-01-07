package stagedsynctest

import (
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon/consensus/bor/borcfg"
	"github.com/ledgerwatch/erigon/params"
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
