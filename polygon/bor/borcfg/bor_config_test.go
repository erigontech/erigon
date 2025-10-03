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

	"github.com/erigontech/erigon/common"
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

func TestCalculateCoinbase(t *testing.T) {
	t.Parallel()
	addr1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	addr2 := common.HexToAddress("0x2222222222222222222222222222222222222222")
	addr3 := common.HexToAddress("0x3333333333333333333333333333333333333333")
	addr4 := common.HexToAddress("0x4444444444444444444444444444444444444444")
	// Test case 1: Nil coinbase configuration
	t.Run("Nil coinbase configuration", func(t *testing.T) {
		config := &BorConfig{}

		result := config.CalculateCoinbase(100)
		expected := common.HexToAddress("0x0000000000000000000000000000000000000000")

		if result != expected {
			t.Errorf("Expected %s, got %s", expected, result)
		}
	})

	// Test case 4: Multiple coinbase addresses with block transitions
	t.Run("Multiple coinbase addresses", func(t *testing.T) {
		config := &BorConfig{
			Coinbase: map[string]common.Address{
				"0":     addr1,
				"1000":  addr2,
				"5000":  addr3,
				"10000": addr4,
			},
		}

		testCases := []struct {
			blockNumber uint64
			expected    common.Address
			description string
		}{
			{0, addr1, "At genesis block"},
			{500, addr1, "Before first transition"},
			{999, addr1, "Just before first transition"},
			{1000, addr2, "At first transition"},
			{1001, addr2, "Just after first transition"},
			{3000, addr2, "Between first and second transition"},
			{4999, addr2, "Just before second transition"},
			{5000, addr3, "At second transition"},
			{7500, addr3, "Between second and third transition"},
			{9999, addr3, "Just before third transition"},
			{10000, addr4, "At third transition"},
			{15000, addr4, "After final transition"},
			{999999, addr4, "Far beyond final transition"},
		}

		for _, tc := range testCases {
			result := config.CalculateCoinbase(tc.blockNumber)
			if result != tc.expected {
				t.Errorf("Block %d (%s): expected %s, got %s",
					tc.blockNumber, tc.description, tc.expected, result)
			}
		}
	})
}
