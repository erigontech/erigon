// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sync

import "github.com/erigontech/erigon/polygon/heimdall"

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

func (cache *SpansCache) IsEmpty() bool {
	return len(cache.spans) == 0
}
