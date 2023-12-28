package span

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon-lib/chain"
)

func TestSpanNumAt(t *testing.T) {
	assert.Equal(t, uint64(0), NumAt(0))
	assert.Equal(t, uint64(0), NumAt(1))
	assert.Equal(t, uint64(0), NumAt(2))
	assert.Equal(t, uint64(0), NumAt(zerothSpanEnd))
	assert.Equal(t, uint64(1), NumAt(zerothSpanEnd+1))
	assert.Equal(t, uint64(1), NumAt(zerothSpanEnd+2))
	assert.Equal(t, uint64(1), NumAt(6655))
	assert.Equal(t, uint64(2), NumAt(6656))
	assert.Equal(t, uint64(2), NumAt(6657))
	assert.Equal(t, uint64(2), NumAt(13055))
	assert.Equal(t, uint64(3), NumAt(13056))
	assert.Equal(t, uint64(6839), NumAt(43763456))
}

func TestSpanEndBlockNum(t *testing.T) {
	assert.Equal(t, uint64(zerothSpanEnd), EndBlockNum(0))
	assert.Equal(t, uint64(6655), EndBlockNum(1))
	assert.Equal(t, uint64(13055), EndBlockNum(2))
	assert.Equal(t, uint64(43769855), EndBlockNum(6839))
}

func TestBlockInLastSprintOfSpan(t *testing.T) {
	config := &chain.BorConfig{
		Sprint: map[string]uint64{
			"0": 16,
		},
	}
	assert.True(t, BlockInLastSprintOfSpan(6640, config))
	assert.True(t, BlockInLastSprintOfSpan(6645, config))
	assert.True(t, BlockInLastSprintOfSpan(6655, config))
	assert.False(t, BlockInLastSprintOfSpan(6639, config))
	assert.False(t, BlockInLastSprintOfSpan(6656, config))
}
