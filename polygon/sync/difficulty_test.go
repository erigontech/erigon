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
	"context"
	"errors"
	"testing"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

type mockBlockProducersReader struct {
	producers *mockValidatorSet
	err       error
}

func (r *mockBlockProducersReader) Producers(_ context.Context, _ uint64) (validatorSet, error) {
	if r.err != nil {
		return nil, r.err
	}

	return r.producers, nil
}

type mockValidatorSet struct {
	signers   []libcommon.Address
	sprintNum int
}

func (mvs *mockValidatorSet) IncrementProposerPriority(times int) {
	mvs.sprintNum = times
}

func (mvs *mockValidatorSet) GetSignerSuccessionNumber(signer libcommon.Address, _ uint64) (int, error) {
	var i int
	for (i < len(mvs.signers)) && (mvs.signers[i] != signer) {
		i++
	}

	sprintOffset := mvs.sprintNum % len(mvs.signers)
	var delta int
	if i >= sprintOffset {
		delta = i - sprintOffset
	} else {
		delta = i + len(mvs.signers) - sprintOffset
	}

	return delta, nil
}

func (mvs *mockValidatorSet) Difficulty(signer libcommon.Address) (uint64, error) {
	delta, err := mvs.GetSignerSuccessionNumber(signer, 0)
	if err != nil {
		return 0, nil
	}
	return uint64(len(mvs.signers) - delta), nil
}

func TestSignerDifficulty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	borConfig := borcfg.BorConfig{
		Sprint: map[string]uint64{"0": 16},
	}
	signers := []libcommon.Address{
		libcommon.HexToAddress("00"),
		libcommon.HexToAddress("01"),
		libcommon.HexToAddress("02"),
	}
	blockProducersReader := &mockBlockProducersReader{
		producers: &mockValidatorSet{
			signers: signers,
		},
	}
	signaturesCache, err := lru.NewARC[libcommon.Hash, libcommon.Address](InMemorySignatures)
	require.NoError(t, err)
	calc := &DifficultyCalculator{
		borConfig:            &borConfig,
		signaturesCache:      signaturesCache,
		blockProducersReader: blockProducersReader,
	}

	var d uint64

	// sprint 0
	d, _ = calc.signerDifficulty(ctx, signers[0], 0)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(ctx, signers[0], 1)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(ctx, signers[0], 15)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(ctx, signers[1], 0)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(ctx, signers[1], 1)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(ctx, signers[1], 15)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(ctx, signers[2], 0)
	assert.Equal(t, uint64(1), d)

	d, _ = calc.signerDifficulty(ctx, signers[2], 1)
	assert.Equal(t, uint64(1), d)

	d, _ = calc.signerDifficulty(ctx, signers[2], 15)
	assert.Equal(t, uint64(1), d)

	// sprint 1
	d, _ = calc.signerDifficulty(ctx, signers[1], 16)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(ctx, signers[2], 16)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(ctx, signers[0], 16)
	assert.Equal(t, uint64(1), d)

	// sprint 2
	d, _ = calc.signerDifficulty(ctx, signers[2], 32)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(ctx, signers[0], 32)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(ctx, signers[1], 32)
	assert.Equal(t, uint64(1), d)

	// sprint 3
	d, _ = calc.signerDifficulty(ctx, signers[0], 48)
	assert.Equal(t, uint64(3), d)

	d, _ = calc.signerDifficulty(ctx, signers[1], 48)
	assert.Equal(t, uint64(2), d)

	d, _ = calc.signerDifficulty(ctx, signers[2], 48)
	assert.Equal(t, uint64(1), d)
}

func TestHeaderDifficultyNoSignature(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	borConfig := borcfg.BorConfig{}
	signaturesCache, err := lru.NewARC[libcommon.Hash, libcommon.Address](InMemorySignatures)
	require.NoError(t, err)
	blockProducersReader := &mockBlockProducersReader{}
	calc := &DifficultyCalculator{
		borConfig:            &borConfig,
		signaturesCache:      signaturesCache,
		blockProducersReader: blockProducersReader,
	}

	_, err = calc.HeaderDifficulty(ctx, new(types.Header))
	require.ErrorContains(t, err, "signature suffix missing")
}

func TestSignerDifficultyBlockProducersNotFound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	borConfig := borcfg.BorConfig{}
	signaturesCache, err := lru.NewARC[libcommon.Hash, libcommon.Address](InMemorySignatures)
	require.NoError(t, err)
	blockProducersReader := &mockBlockProducersReader{
		err: errors.New("block producer not found for blockNum"),
	}
	calc := &DifficultyCalculator{
		borConfig:            &borConfig,
		signaturesCache:      signaturesCache,
		blockProducersReader: blockProducersReader,
	}

	_, err = calc.signerDifficulty(ctx, libcommon.HexToAddress("00"), 0)
	require.ErrorContains(t, err, "block producer not found for blockNum")
}
