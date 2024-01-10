package borcfg

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCalculateSprintNumber(t *testing.T) {
	cfg := BorConfig{
		Sprint: map[string]uint64{
			"0":   64,
			"256": 16,
		},
	}

	examples := map[uint64]uint64{
		0:   0,
		1:   0,
		2:   0,
		63:  0,
		64:  1,
		65:  1,
		66:  1,
		127: 1,
		128: 2,
		191: 2,
		192: 3,
		255: 3,
		256: 4,
		257: 4,
		258: 4,
		271: 4,
		272: 5,
		273: 5,
		274: 5,
		287: 5,
		288: 6,
		303: 6,
		304: 7,
		319: 7,
		320: 8,
	}

	for blockNumber, expectedSprintNumber := range examples {
		assert.Equal(t, expectedSprintNumber, cfg.CalculateSprintNumber(blockNumber), blockNumber)
	}
}
