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

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

type testValidatorSetInterface struct {
	signers   []libcommon.Address
	sprintNum int
}

func (v *testValidatorSetInterface) IncrementProposerPriority(times int) {
	v.sprintNum = times
}

func (v *testValidatorSetInterface) GetSignerSuccessionNumber(signer libcommon.Address, number uint64) (int, error) {
	var i int
	for (i < len(v.signers)) && (v.signers[i] != signer) {
		i++
	}

	sprintOffset := v.sprintNum % len(v.signers)
	var delta int
	if i >= sprintOffset {
		delta = i - sprintOffset
	} else {
		delta = i + len(v.signers) - sprintOffset
	}

	return delta, nil
}

func (v *testValidatorSetInterface) Difficulty(signer libcommon.Address) (uint64, error) {
	delta, err := v.GetSignerSuccessionNumber(signer, 0)
	if err != nil {
		return 0, nil
	}
	return uint64(len(v.signers) - delta), nil
}

func TestSignerDifficulty(t *testing.T) {
	borConfig := borcfg.BorConfig{
		Sprint: map[string]uint64{"0": 16},
	}
	signers := []libcommon.Address{
		libcommon.HexToAddress("00"),
		libcommon.HexToAddress("01"),
		libcommon.HexToAddress("02"),
	}
	validatorSetFactory := func(uint64) validatorSetInterface { return &testValidatorSetInterface{signers: signers} }
	calc := NewDifficultyCalculator(&borConfig, nil, validatorSetFactory, nil).(*difficultyCalculator)

	var d uint64

	// sprint 0
	d, _ = calc.signerDifficulty(signers[0], 0)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(signers[0], 1)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(signers[0], 15)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(signers[1], 0)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(signers[1], 1)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(signers[1], 15)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(signers[2], 0)
	assert.Equal(t, uint64(1), d)

	d, _ = calc.signerDifficulty(signers[2], 1)
	assert.Equal(t, uint64(1), d)

	d, _ = calc.signerDifficulty(signers[2], 15)
	assert.Equal(t, uint64(1), d)

	// sprint 1
	d, _ = calc.signerDifficulty(signers[1], 16)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(signers[2], 16)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(signers[0], 16)
	assert.Equal(t, uint64(1), d)

	// sprint 2
	d, _ = calc.signerDifficulty(signers[2], 32)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(signers[0], 32)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(signers[1], 32)
	assert.Equal(t, uint64(1), d)

	// sprint 3
	d, _ = calc.signerDifficulty(signers[0], 48)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(signers[1], 48)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(signers[2], 48)
	assert.Equal(t, uint64(1), d)
}

func TestHeaderDifficultyNoSignature(t *testing.T) {
	borConfig := borcfg.BorConfig{}
	spans := NewSpansCache()
	calc := NewDifficultyCalculator(&borConfig, spans, nil, nil)

	_, err := calc.HeaderDifficulty(new(types.Header))
	require.ErrorContains(t, err, "signature suffix missing")
}

func TestSignerDifficultyNoSpan(t *testing.T) {
	borConfig := borcfg.BorConfig{}
	spans := NewSpansCache()
	calc := NewDifficultyCalculator(&borConfig, spans, nil, nil).(*difficultyCalculator)

	_, err := calc.signerDifficulty(libcommon.HexToAddress("00"), 0)
	require.ErrorContains(t, err, "no span")
}
