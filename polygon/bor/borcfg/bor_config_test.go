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

package borcfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
