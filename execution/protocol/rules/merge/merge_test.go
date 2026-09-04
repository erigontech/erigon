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

package merge

import (
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type readerMock struct {
	config *chain.Config
}

func (r readerMock) Config() *chain.Config {
	return r.config
}

func (r readerMock) CurrentHeader() *types.Header {
	return nil
}

func (cr readerMock) CurrentFinalizedHeader() *types.Header {
	return nil
}

func (cr readerMock) CurrentSafeHeader() *types.Header {
	return nil
}

func (r readerMock) GetHeader(common.Hash, uint64) *types.Header {
	return nil
}

func (r readerMock) GetHeaderByNumber(uint64) *types.Header {
	return nil
}

func (r readerMock) GetHeaderByHash(common.Hash) *types.Header {
	return nil
}

func (r readerMock) GetTd(common.Hash, uint64) *uint256.Int {
	return nil
}

func (r readerMock) FrozenBlocks() uint64 {
	return 0
}

// The thing only that changes between normal ethash checks other than POW, is difficulty
// and nonce so we are gonna test those
func TestVerifyHeaderDifficulty(t *testing.T) {
	header := &types.Header{
		Difficulty: *common.Num1,
		Time:       1,
	}

	parent := &types.Header{}

	var eth1Engine rules.Engine
	mergeEngine := New(eth1Engine)

	err := mergeEngine.verifyHeader(readerMock{}, header, parent)
	if !errors.Is(err, errInvalidDifficulty) {
		if err != nil {
			t.Fatalf("Merge engine should not accept non-zero difficulty, got %s", err.Error())
		} else {
			t.Fatalf("Merge engine should not accept non-zero difficulty")
		}
	}
}

func TestVerifyHeaderNonce(t *testing.T) {
	header := &types.Header{
		Nonce:      types.BlockNonce{1, 0, 0, 0, 0, 0, 0, 0},
		Difficulty: *common.Num0,
		Time:       1,
	}

	parent := &types.Header{}

	var eth1Engine rules.Engine
	mergeEngine := New(eth1Engine)

	err := mergeEngine.verifyHeader(readerMock{}, header, parent)
	if !errors.Is(err, errInvalidNonce) {
		if err != nil {
			t.Fatalf("Merge engine should not accept non-zero difficulty, got %s", err.Error())
		} else {
			t.Fatalf("Merge engine should not accept non-zero difficulty")
		}
	}
}

func TestVerifyHeaderRequiresSlotNumberAfterAmsterdam(t *testing.T) {
	t.Parallel()

	config := &chain.Config{
		LondonBlock:   common.NewUint64(0),
		ShanghaiTime:  common.NewUint64(0),
		CancunTime:    common.NewUint64(0),
		PragueTime:    common.NewUint64(0),
		AmsterdamTime: common.NewUint64(0),
	}
	zero := uint64(0)
	baseFee := uint256.NewInt(1)
	parent := &types.Header{
		Number:        *uint256.NewInt(0),
		Time:          1,
		GasLimit:      30_000_000,
		GasUsed:       15_000_000,
		BaseFee:       baseFee,
		BlobGasUsed:   &zero,
		ExcessBlobGas: &zero,
	}
	emptyHash := common.Hash{}
	header := &types.Header{
		Number:                *uint256.NewInt(1),
		Time:                  2,
		GasLimit:              parent.GasLimit,
		BaseFee:               new(uint256.Int).Set(baseFee),
		Difficulty:            *ProofOfStakeDifficulty,
		UncleHash:             empty.UncleHash,
		WithdrawalsHash:       &emptyHash,
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
		ParentBeaconBlockRoot: &emptyHash,
		RequestsHash:          &emptyHash,
		BlockAccessListHash:   &emptyHash,
	}

	err := New(nil).verifyHeader(readerMock{config: config}, header, parent)
	require.ErrorIs(t, err, rules.ErrMissingSlotNumber)
}

func TestNullParentBeaconBlockRootDoesNotPanic(t *testing.T) {
	chainConfig := chainspec.Mainnet.Config
	header := &types.Header{ // fake PoS header *after* Cancun fork
		Difficulty: *ProofOfStakeDifficulty,
		Time:       *chainConfig.CancunTime + 1,
	}
	logger := log.New()
	chainReader := consensuschain.NewReader(chainConfig, nil, nil, logger) // tx and blockReader don't care
	systemCallCustom := func(contract accounts.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		return nil, nil
	}
	var intraBlockState state.IntraBlockState // don't care
	var tracer tracing.Hooks                  // don't care
	var eth1Engine rules.Engine
	mergeEngine := New(eth1Engine)
	err := mergeEngine.Initialize(chainConfig, chainReader, header, &intraBlockState, systemCallCustom, logger, &tracer)
	assert.NoError(t, err)
}

// withdrawalErrReader fails the account read for one address, standing in for a
// domain read failure on a cold withdrawal recipient.
type withdrawalErrReader struct {
	state.StateReader
	fail accounts.Address
	err  error
}

func (r withdrawalErrReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if addr == r.fail {
		return nil, r.err
	}
	return r.StateReader.ReadAccountData(addr)
}

// TestFinalizeWithdrawalStateErrorPropagates verifies a state failure while
// crediting a withdrawal aborts Finalize instead of silently skipping it.
func TestFinalizeWithdrawalStateErrorPropagates(t *testing.T) {
	chainConfig := chainspec.Mainnet.Config
	header := &types.Header{
		Difficulty: *ProofOfStakeDifficulty,
		Time:       *chainConfig.CancunTime + 1,
	}
	recipient := common.HexToAddress("0x2222222222222222222222222222222222222222")
	boom := errors.New("domain read failed")
	ibs := state.New(withdrawalErrReader{
		StateReader: state.NewNoopReader(),
		fail:        accounts.InternAddress(recipient),
		err:         boom,
	})

	logger := log.New()
	var eth1Engine rules.Engine
	mergeEngine := New(eth1Engine)
	withdrawals := []*types.Withdrawal{{Index: 7, Address: recipient, Amount: 1_000}}
	syscall := func(accounts.Address, []byte) ([]byte, error) { return nil, nil }

	_, err := mergeEngine.Finalize(chainConfig, header, ibs, nil, nil, withdrawals,
		consensuschain.NewReader(chainConfig, nil, nil, logger), syscall, false, logger)

	require.ErrorIs(t, err, boom)
	require.Contains(t, err.Error(), "withdrawal 7")
}

type stateDerivedL2 struct{}

func (stateDerivedL2) Name() string { return "statederived" }

func (stateDerivedL2) ResolveRules(l2Version, _, _ uint64, r *chain.Rules) {
	r.L2Version = l2Version
}

type blockDerivedL2 struct{}

func (blockDerivedL2) Name() string { return "blockderived" }

func (blockDerivedL2) ResolveRules(_, blockNum, _ uint64, r *chain.Rules) {
	if blockNum >= 20_000_000 {
		r.L2Version = 50
	} else {
		r.L2Version = 30
	}
}

func TestInitializeTracesTheRulesTheSystemCallResolves(t *testing.T) {
	for _, tc := range []struct {
		name     string
		l2       chain.L2Config
		blockNum uint64
	}{
		{"version from block number, below the ladder step", blockDerivedL2{}, 15_000_000},
		{"version from block number, above the ladder step", blockDerivedL2{}, 21_000_000},
		{"version from state, which system calls never carry", stateDerivedL2{}, 21_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cancunTime := uint64(0)
			chainConfig := chain.Config{
				ChainID:    uint256.NewInt(1337),
				CancunTime: &cancunTime,
				L2:         tc.l2,
			}
			beaconRoot := common.HexToHash("0xbeac07")
			header := &types.Header{
				Difficulty:            *ProofOfStakeDifficulty,
				Number:                *uint256.NewInt(tc.blockNum),
				Time:                  1,
				ParentBeaconBlockRoot: &beaconRoot,
			}

			var seen *tracing.VMContext
			tracer := tracing.Hooks{
				OnSystemCallStartV2: func(env *tracing.VMContext) { seen = env },
			}

			logger := log.New()
			chainReader := consensuschain.NewReader(&chainConfig, nil, nil, logger)
			systemCallCustom := func(accounts.Address, []byte, *state.IntraBlockState, *types.Header, bool) ([]byte, error) {
				return nil, nil
			}
			var intraBlockState state.IntraBlockState
			var eth1Engine rules.Engine

			require.NoError(t, New(eth1Engine).Initialize(&chainConfig, chainReader, header,
				&intraBlockState, systemCallCustom, logger, &tracer))

			require.NotNil(t, seen, "the Cancun system call must reach OnSystemCallStartV2")
			require.NotNil(t, seen.Rules, "the traced context must carry the rules, not the ingredients to rebuild them")

			execCtx := evmtypes.BlockContext{BlockNumber: header.Number.Uint64(), Time: header.Time}
			require.Equal(t, execCtx.Rules(&chainConfig).L2Version, seen.Rules.L2Version,
				"the traced rules must be the ones the system call's own EVM resolves")
		})
	}
}

func TestInitializeTracesABlockDerivedVersionChange(t *testing.T) {
	traced := func(blockNum uint64) uint64 {
		cancunTime := uint64(0)
		chainConfig := chain.Config{ChainID: uint256.NewInt(1337), CancunTime: &cancunTime, L2: blockDerivedL2{}}
		beaconRoot := common.HexToHash("0xbeac07")
		header := &types.Header{
			Difficulty:            *ProofOfStakeDifficulty,
			Number:                *uint256.NewInt(blockNum),
			Time:                  1,
			ParentBeaconBlockRoot: &beaconRoot,
		}
		var seen *tracing.VMContext
		tracer := tracing.Hooks{OnSystemCallStartV2: func(env *tracing.VMContext) { seen = env }}
		logger := log.New()
		var intraBlockState state.IntraBlockState
		var eth1Engine rules.Engine
		require.NoError(t, New(eth1Engine).Initialize(&chainConfig,
			consensuschain.NewReader(&chainConfig, nil, nil, logger), header, &intraBlockState,
			func(accounts.Address, []byte, *state.IntraBlockState, *types.Header, bool) ([]byte, error) {
				return nil, nil
			}, logger, &tracer))
		require.NotNil(t, seen.Rules)
		return seen.Rules.L2Version
	}

	require.Equal(t, uint64(30), traced(15_000_000))
	require.Equal(t, uint64(50), traced(21_000_000),
		"a version that moves with the block must move in the trace too")
}
