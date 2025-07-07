/*
   Copyright 2023 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package chain

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/common"
)

func TestBorKeyValueConfigHelper(t *testing.T) {
	backupMultiplier := map[string]uint64{
		"0":        2,
		"25275000": 5,
		"29638656": 2,
	}
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 0), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 1), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 25275000-1), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 25275000), uint64(5))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 25275000+1), uint64(5))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 29638656-1), uint64(5))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 29638656), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(backupMultiplier, 29638656+1), uint64(2))

	config := map[string]uint64{
		"0":         1,
		"90000000":  2,
		"100000000": 3,
	}
	assert.Equal(t, borKeyValueConfigHelper(config, 0), uint64(1))
	assert.Equal(t, borKeyValueConfigHelper(config, 1), uint64(1))
	assert.Equal(t, borKeyValueConfigHelper(config, 90000000-1), uint64(1))
	assert.Equal(t, borKeyValueConfigHelper(config, 90000000), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(config, 90000000+1), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(config, 100000000-1), uint64(2))
	assert.Equal(t, borKeyValueConfigHelper(config, 100000000), uint64(3))
	assert.Equal(t, borKeyValueConfigHelper(config, 100000000+1), uint64(3))

	address1 := common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38")
	address2 := common.HexToAddress("0x617b94CCCC2511808A3C9478ebb96f455CF167aA")

	burntContract := map[string]common.Address{
		"22640000": address1,
		"41874000": address2,
	}
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 22640000), address1)
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 22640000+1), address1)
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 41874000-1), address1)
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 41874000), address2)
	assert.Equal(t, borKeyValueConfigHelper(burntContract, 41874000+1), address2)
}

func TestNilBlobSchedule(t *testing.T) {
	var b *BlobSchedule

	// Original EIP-4844 values
	isPrague := false
	assert.Equal(t, uint64(3), b.TargetBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(6), b.MaxBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(3338477), b.BaseFeeUpdateFraction(isPrague))

	// EIP-7691: Blob throughput increase
	isPrague = true
	assert.Equal(t, uint64(6), b.TargetBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(9), b.MaxBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(5007716), b.BaseFeeUpdateFraction(isPrague))
}

func TestIsForked(t *testing.T) {
	tests := map[string]struct {
		s        *big.Int
		head     uint64
		expected bool
	}{
		"nil fork block": {
			s:        nil,
			head:     100,
			expected: false,
		},
		"fork at block 0": {
			s:        big.NewInt(0),
			head:     100,
			expected: true,
		},
		"fork at block 50, head at 100": {
			s:        big.NewInt(50),
			head:     100,
			expected: true,
		},
		"fork at block 150, head at 100": {
			s:        big.NewInt(150),
			head:     100,
			expected: false,
		},
		"fork at max uint64": {
			s:        new(big.Int).SetUint64(^uint64(0)),
			head:     100,
			expected: false,
		},
		"fork at max uint64, head at max uint64": {
			s:        new(big.Int).SetUint64(^uint64(0)),
			head:     ^uint64(0),
			expected: true,
		},
		"fork at value larger than max uint64": {
			s:        new(big.Int).Mul(new(big.Int).SetUint64(^uint64(0)), big.NewInt(2)),
			head:     100,
			expected: false,
		},
		"fork at extremely large value (like in hermez-dev.json)": {
			s: func() *big.Int {
				n := new(big.Int)
				n.SetString("9999999999999999999999999999999999999999999999999", 10)
				return n
			}(),
			head:     1,
			expected: false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			result := isForked(tt.s, tt.head)
			if result != tt.expected {
				t.Errorf("isForked(%v, %d) = %v, want %v", tt.s, tt.head, result, tt.expected)
			}
		})
	}
}
