package sync

import "github.com/ledgerwatch/erigon/polygon/heimdall"

type SpansCache struct {
	spans map[uint64]*heimdall.Span
}

func NewSpansCache() *SpansCache {
	return &SpansCache{
		spans: make(map[uint64]*heimdall.Span),
	}
}

func (cache *SpansCache) Add(span *heimdall.Span) {
	cache.spans[span.StartBlock] = span
}

// SpanAt finds a span that contains blockNum.
func (cache *SpansCache) SpanAt(blockNum uint64) *heimdall.Span {
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
