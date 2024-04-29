package sync

import (
	lru "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

type CanonicalChainBuilderFactory func(root *types.Header, span *heimdall.Span) CanonicalChainBuilder

func NewCanonicalChainBuilderFactory(
	chainConfig *chain.Config,
	borConfig *borcfg.BorConfig,
	spansCache *SpansCache,
) CanonicalChainBuilderFactory {
	signaturesCache, err := lru.NewARC[common.Hash, common.Address](stagedsync.InMemorySignatures)
	if err != nil {
		panic(err)
	}

	difficultyCalculator := NewDifficultyCalculator(borConfig, spansCache, nil, signaturesCache)
	headerTimeValidator := NewHeaderTimeValidator(borConfig, spansCache, nil, signaturesCache)
	headerValidator := NewHeaderValidator(chainConfig, borConfig, headerTimeValidator)

	return func(root *types.Header, span *heimdall.Span) CanonicalChainBuilder {
		if span == nil {
			panic("sync.Service: ccBuilderFactory - span is nil")
		}
		if spansCache.IsEmpty() {
			panic("sync.Service: ccBuilderFactory - spansCache is empty")
		}
		return NewCanonicalChainBuilder(
			root,
			difficultyCalculator,
			headerValidator,
			spansCache,
		)
	}
}
