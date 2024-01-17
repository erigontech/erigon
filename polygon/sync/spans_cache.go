package sync

import (
	heimdallspan "github.com/ledgerwatch/erigon/polygon/heimdall"
)

type SpansCache struct {
	spans map[uint64]*heimdallspan.HeimdallSpan
}

func NewSpansCache() *SpansCache {
	return &SpansCache{
		spans: make(map[uint64]*heimdallspan.HeimdallSpan),
	}
}

func (cache *SpansCache) Add(span *heimdallspan.HeimdallSpan) {
	cache.spans[span.StartBlock] = span
}

// SpanAt finds a span that contains blockNum.
func (cache *SpansCache) SpanAt(blockNum uint64) *heimdallspan.HeimdallSpan {
	for _, span := range cache.spans {
		if (span.StartBlock <= blockNum) && (blockNum <= span.EndBlock) {
			return span
		}
	}
	return nil
}

// Prune removes spans that ended before blockNum.
func (cache *SpansCache) Prune(blockNum uint64) {
	for key, span := range cache.spans {
		if span.EndBlock < blockNum {
			delete(cache.spans, key)
		}
	}
}
