// Copyright 2025 The Erigon Authors
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

package protocol_test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	liblog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"

	"github.com/erigontech/erigon/common"
)

// testTransitionEnv holds the state needed for a single transition test run.
type testTransitionEnv struct {
	state *state.IntraBlockState
	evm   *vm.EVM
	rules *chain.Rules
}

// setupTransitionEnv creates a minimal EVM environment with a funded sender
// and an optional TransitionHasher attached.
func setupTransitionEnv(t *testing.T, attachTransitionHasher bool) (*testTransitionEnv, accounts.Address) {
	t.Helper()

	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(t.Context(), tx, liblog.New())
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	txNum, _, err := sd.SeekCommitment(t.Context(), tx)
	require.NoError(t, err)

	r := state.NewReaderV3(sd.AsGetter(tx))
	w := state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	s := state.New(r)

	sender := accounts.InternAddress(common.HexToAddress("0xaaaa"))
	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	coinbase := accounts.InternAddress(common.HexToAddress("0xcccc"))

	// Fund sender with 100 ETH
	s.CreateAccount(sender, true)
	s.AddBalance(sender, *uint256.NewInt(0).Mul(uint256.NewInt(100), uint256.NewInt(1e18)), tracing.BalanceChangeUnspecified)

	// Create recipient account
	s.CreateAccount(recipient, true)

	// Create coinbase account
	s.CreateAccount(coinbase, true)

	baseFee := uint256.NewInt(10_000_000_000) // 10 gwei

	vmctx := evmtypes.BlockContext{
		CanTransfer: protocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    coinbase,
		GasLimit:    30_000_000,
		BlockNumber: 1,
		Time:        1000,
		BaseFee:     *baseFee,
	}

	rules := vmctx.Rules(chain.AllProtocolChanges)
	err = s.CommitBlock(rules, w)
	require.NoError(t, err)

	txContext := evmtypes.TxContext{
		TxHash: common.HexToHash("0xdeadbeef"),
		Origin: sender,
	}

	evm := vm.NewEVM(vmctx, txContext, s, chain.AllProtocolChanges, vm.Config{})

	if attachTransitionHasher {
		th := vm.GetTransitionHasher()
		th.Reset()
		evm.SetTransitionHasher(th)
		t.Cleanup(func() { vm.PutTransitionHasher(th) })
	}

	return &testTransitionEnv{state: s, evm: evm, rules: rules}, sender
}

// newSimpleTransferMsg creates a message for a simple ETH transfer.
func newSimpleTransferMsg(from, to accounts.Address, nonce uint64, value, gasPrice, feeCap, tipCap *uint256.Int) *types.Message {
	return types.NewMessage(
		from, to, nonce, value,
		21000,    // gasLimit
		gasPrice, // gasPrice
		feeCap, tipCap,
		nil,  // data
		nil,  // accessList
		true, // checkNonce
		false, false, false, nil,
	)
}

func TestTransitionHasher_Integration_SimpleTransfer(t *testing.T) {
	env, sender := setupTransitionEnv(t, true)

	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	feeCap := uint256.NewInt(30_000_000_000)           // 30 gwei
	tipCap := uint256.NewInt(2_000_000_000)            // 2 gwei
	value := uint256.NewInt(1_000_000_000_000_000_000) // 1 ETH

	msg := newSimpleTransferMsg(sender, recipient, 0, value, feeCap, feeCap, tipCap)

	gp := new(protocol.GasPool)
	gp.AddGas(30_000_000)

	result, err := protocol.ApplyMessage(env.evm, msg, gp, true, false, nil)
	require.NoError(t, err)
	require.False(t, result.Failed(), "simple transfer should succeed")
	require.NotEqual(t, common.Hash{}, result.TransitionHash, "transition hash must be non-zero")

	t.Logf("transition hash: %x", result.TransitionHash)
}

func TestTransitionHasher_Integration_Determinism(t *testing.T) {
	// Run the same transition twice and verify identical hashes.
	runTransfer := func() common.Hash {
		env, sender := setupTransitionEnv(t, true)
		recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
		feeCap := uint256.NewInt(30_000_000_000)
		tipCap := uint256.NewInt(2_000_000_000)
		value := uint256.NewInt(1_000_000_000_000_000_000)

		msg := newSimpleTransferMsg(sender, recipient, 0, value, feeCap, feeCap, tipCap)

		gp := new(protocol.GasPool)
		gp.AddGas(30_000_000)

		result, err := protocol.ApplyMessage(env.evm, msg, gp, true, false, nil)
		require.NoError(t, err)
		require.False(t, result.Failed())
		return result.TransitionHash
	}

	hash1 := runTransfer()
	hash2 := runTransfer()
	require.Equal(t, hash1, hash2, "same transition must produce identical hashes")
	require.NotEqual(t, common.Hash{}, hash1)
}

func TestTransitionHasher_Integration_DifferentValue(t *testing.T) {
	runTransfer := func(value *uint256.Int) common.Hash {
		env, sender := setupTransitionEnv(t, true)
		recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
		feeCap := uint256.NewInt(30_000_000_000)
		tipCap := uint256.NewInt(2_000_000_000)

		msg := newSimpleTransferMsg(sender, recipient, 0, value, feeCap, feeCap, tipCap)

		gp := new(protocol.GasPool)
		gp.AddGas(30_000_000)

		result, err := protocol.ApplyMessage(env.evm, msg, gp, true, false, nil)
		require.NoError(t, err)
		require.False(t, result.Failed())
		return result.TransitionHash
	}

	hash1 := runTransfer(uint256.NewInt(1_000_000_000_000_000_000)) // 1 ETH
	hash2 := runTransfer(uint256.NewInt(2_000_000_000_000_000_000)) // 2 ETH
	require.NotEqual(t, hash1, hash2, "different transfer values must produce different hashes")
}

func TestTransitionHasher_Integration_Disabled(t *testing.T) {
	// When no TransitionHasher is attached, the hash should be zero.
	env, sender := setupTransitionEnv(t, false)

	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	feeCap := uint256.NewInt(30_000_000_000)
	tipCap := uint256.NewInt(2_000_000_000)
	value := uint256.NewInt(1_000_000_000_000_000_000)

	msg := newSimpleTransferMsg(sender, recipient, 0, value, feeCap, feeCap, tipCap)

	gp := new(protocol.GasPool)
	gp.AddGas(30_000_000)

	result, err := protocol.ApplyMessage(env.evm, msg, gp, true, false, nil)
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.Equal(t, common.Hash{}, result.TransitionHash, "disabled hasher must produce zero hash")
}

func TestTransitionHasher_Integration_DifferentRecipient(t *testing.T) {
	runTransfer := func(to common.Address) common.Hash {
		env, sender := setupTransitionEnv(t, true)
		recipient := accounts.InternAddress(to)
		feeCap := uint256.NewInt(30_000_000_000)
		tipCap := uint256.NewInt(2_000_000_000)
		value := uint256.NewInt(1_000_000_000_000_000_000)

		msg := newSimpleTransferMsg(sender, recipient, 0, value, feeCap, feeCap, tipCap)

		gp := new(protocol.GasPool)
		gp.AddGas(30_000_000)

		result, err := protocol.ApplyMessage(env.evm, msg, gp, true, false, nil)
		require.NoError(t, err)
		require.False(t, result.Failed())
		return result.TransitionHash
	}

	hash1 := runTransfer(common.HexToAddress("0xbbbb"))
	hash2 := runTransfer(common.HexToAddress("0xdddd"))
	require.NotEqual(t, hash1, hash2, "different recipients must produce different hashes")
}

func TestTransitionHasher_Integration_WithAccessList(t *testing.T) {
	env, sender := setupTransitionEnv(t, true)

	recipient := accounts.InternAddress(common.HexToAddress("0xbbbb"))
	feeCap := uint256.NewInt(30_000_000_000)
	tipCap := uint256.NewInt(2_000_000_000)
	value := uint256.NewInt(1_000_000_000_000_000_000)

	accessList := types.AccessList{
		{
			Address:     common.HexToAddress("0xeeee"),
			StorageKeys: []common.Hash{common.HexToHash("0x01"), common.HexToHash("0x02")},
		},
	}

	msg := types.NewMessage(
		sender, recipient, 0, value,
		30000,  // higher gas for access list
		feeCap, // gasPrice
		feeCap, tipCap,
		nil,        // data
		accessList, // accessList
		true,       // checkNonce
		false, false, false, nil,
	)

	gp := new(protocol.GasPool)
	gp.AddGas(30_000_000)

	result, err := protocol.ApplyMessage(env.evm, msg, gp, true, false, nil)
	require.NoError(t, err)
	require.False(t, result.Failed())
	require.NotEqual(t, common.Hash{}, result.TransitionHash, "transition with access list must produce non-zero hash")

	// Verify it differs from no-access-list version
	env2, sender2 := setupTransitionEnv(t, true)
	msg2 := newSimpleTransferMsg(sender2, recipient, 0, value, feeCap, feeCap, tipCap)

	gp2 := new(protocol.GasPool)
	gp2.AddGas(30_000_000)

	result2, err := protocol.ApplyMessage(env2.evm, msg2, gp2, true, false, nil)
	require.NoError(t, err)
	require.NotEqual(t, result.TransitionHash, result2.TransitionHash, "access list must change the transition hash")
}
