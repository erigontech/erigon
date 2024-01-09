package sync

import (
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor/borcfg"
	heimdallspan "github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/core/types"
)

type testValidatorSetInterface struct {
	signers   []libcommon.Address
	sprintNum int
}

func (v *testValidatorSetInterface) IncrementProposerPriority(times int, _ log.Logger) {
	v.sprintNum = times
}

func (v *testValidatorSetInterface) Difficulty(signer libcommon.Address) (uint64, error) {
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

	return uint64(len(v.signers) - delta), nil
}

func TestSignerDifficulty(t *testing.T) {
	borConfig := borcfg.BorConfig{
		Sprint: map[string]uint64{"0": 16},
	}
	span := heimdallspan.HeimdallSpan{}
	signers := []libcommon.Address{
		libcommon.HexToAddress("00"),
		libcommon.HexToAddress("01"),
		libcommon.HexToAddress("02"),
	}
	validatorSetFactory := func() validatorSetInterface { return &testValidatorSetInterface{signers: signers} }
	logger := log.New()
	calc := NewDifficultyCalculator(&borConfig, &span, validatorSetFactory, logger).(*difficultyCalculatorImpl)

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
	span := heimdallspan.HeimdallSpan{}
	logger := log.New()
	calc := NewDifficultyCalculator(&borConfig, &span, nil, logger)

	_, err := calc.HeaderDifficulty(new(types.Header))
	require.ErrorContains(t, err, "signature suffix missing")
}
