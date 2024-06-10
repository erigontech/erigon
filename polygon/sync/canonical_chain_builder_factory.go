package sync

import (
	"context"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

const InMemorySignatures = 4096 // Number of recent block signatures to keep in memory

type CanonicalChainBuilderFactory func(root *types.Header, span *heimdall.Span) CanonicalChainBuilder
type SpanFetcher func(ctx context.Context, blockNum uint64) (*heimdall.Span, error)

func NewCanonicalChainBuilderFactory(
	chainConfig *chain.Config,
	borConfig *borcfg.BorConfig,
	spansCache *SpansCache,
	spanFetcher SpanFetcher,
) CanonicalChainBuilderFactory {
	signaturesCache, err := lru.NewARC[common.Hash, common.Address](InMemorySignatures)
	if err != nil {
		panic(err)
	}

	difficultyCalculator := NewDifficultyCalculator(borConfig, spansCache, nil, signaturesCache)
	headerTimeValidator := NewHeaderTimeValidator(borConfig, spansCache, nil, signaturesCache, spanFetcher)
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
			spanFetcher,
		)
	}
}
