package heimdall

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
)

func TestSpanIDAt(t *testing.T) {
	assert.Equal(t, SpanId(0), SpanIdAt(0))
	assert.Equal(t, SpanId(0), SpanIdAt(1))
	assert.Equal(t, SpanId(0), SpanIdAt(2))
	assert.Equal(t, SpanId(0), SpanIdAt(zerothSpanEnd))
	assert.Equal(t, SpanId(1), SpanIdAt(zerothSpanEnd+1))
	assert.Equal(t, SpanId(1), SpanIdAt(zerothSpanEnd+2))
	assert.Equal(t, SpanId(1), SpanIdAt(6655))
	assert.Equal(t, SpanId(2), SpanIdAt(6656))
	assert.Equal(t, SpanId(2), SpanIdAt(6657))
	assert.Equal(t, SpanId(2), SpanIdAt(13055))
	assert.Equal(t, SpanId(3), SpanIdAt(13056))
	assert.Equal(t, SpanId(6839), SpanIdAt(43763456))
}

func TestSpanEndBlockNum(t *testing.T) {
	assert.Equal(t, uint64(zerothSpanEnd), SpanEndBlockNum(0))
	assert.Equal(t, uint64(6655), SpanEndBlockNum(1))
	assert.Equal(t, uint64(13055), SpanEndBlockNum(2))
	assert.Equal(t, uint64(43769855), SpanEndBlockNum(6839))
}

func TestBlockInLastSprintOfSpan(t *testing.T) {
	config := &borcfg.BorConfig{
		Sprint: map[string]uint64{
			"0": 16,
		},
	}
	assert.True(t, IsBlockInLastSprintOfSpan(6640, config))
	assert.True(t, IsBlockInLastSprintOfSpan(6645, config))
	assert.True(t, IsBlockInLastSprintOfSpan(6655, config))
	assert.False(t, IsBlockInLastSprintOfSpan(6639, config))
	assert.False(t, IsBlockInLastSprintOfSpan(6656, config))
}
