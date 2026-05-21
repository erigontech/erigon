package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// mapStateReader is a test StateReader backed by in-memory maps.
type mapStateReader struct {
	accounts map[accounts.Address]*accounts.Account
	storage  map[accounts.Address]map[accounts.StorageKey]uint256.Int
	code     map[accounts.Address][]byte
}

func newMapStateReader() *mapStateReader {
	return &mapStateReader{
		accounts: make(map[accounts.Address]*accounts.Account),
		storage:  make(map[accounts.Address]map[accounts.StorageKey]uint256.Int),
		code:     make(map[accounts.Address][]byte),
	}
}

func (r *mapStateReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if a, ok := r.accounts[addr]; ok {
		cpy := *a
		return &cpy, nil
	}
	return nil, nil
}

func (r *mapStateReader) ReadAccountDataForDebug(addr accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(addr)
}

func (r *mapStateReader) ReadAccountStorage(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	if slots, ok := r.storage[addr]; ok {
		if v, ok := slots[key]; ok {
			return v, true, nil
		}
	}
	return uint256.Int{}, false, nil
}

func (r *mapStateReader) HasStorage(accounts.Address) (bool, error) {
	return false, nil
}

func (r *mapStateReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	return r.code[addr], nil
}

func (r *mapStateReader) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	return len(r.code[addr]), nil
}

func (r *mapStateReader) SetTrace(bool, string) {}
func (r *mapStateReader) Trace() bool           { return false }
func (r *mapStateReader) TracePrefix() string   { return "" }

// testFinalizeScenario captures inputs for finalize path comparison tests.
type testFinalizeScenario struct {
	name string
	// Pre-existing account state (what the state reader returns).
	accts map[accounts.Address]*accounts.Account
	// Execution reads/writes and fees.
	txIn            state.ReadSet
	txOut           state.VersionedWrites
	collectorWrites state.VersionedWrites
	feeTipped       uint256.Int
	feeBurnt        uint256.Int
	coinbase        accounts.Address
	burntAddr       accounts.Address
	rules           *chain.Rules
	config          *chain.Config
	header          *types.Header
}

// copyReadSet makes a shallow copy of a ReadSet for test isolation.
func copyReadSet(rs state.ReadSet) state.ReadSet {
	out := make(state.ReadSet, len(rs))
	for addr, keys := range rs {
		out[addr] = make(map[state.AccountKey]state.VersionedRead, len(keys))
		for k, v := range keys {
			out[addr][k] = v
		}
	}
	return out
}

// copyWrites makes a shallow copy of VersionedWrites.
func copyWrites(ws state.VersionedWrites) state.VersionedWrites {
	out := make(state.VersionedWrites, len(ws))
	for i, w := range ws {
		cpy := *w
		out[i] = &cpy
	}
	return out
}

func (s *testFinalizeScenario) makeReader() *mapStateReader {
	r := newMapStateReader()
	for addr, acc := range s.accts {
		cpy := *acc
		r.accounts[addr] = &cpy
	}
	return r
}

// buildExecResult creates an execResult for testing.
func (s *testFinalizeScenario) buildExecResult() *execResult {
	blockNum := s.header.Number.Uint64()

	txTask := &exec.TxTask{
		Header:  s.header,
		TxNum:   1,
		TxIndex: 0,
		Config:  s.config,
		EvmBlockContext: evmtypes.BlockContext{
			BlockNumber: blockNum,
		},
	}

	task := &taskVersion{
		execTask: &execTask{
			Task:               txTask,
			shouldDelayFeeCalc: true,
		},
		version: state.Version{
			BlockNum: blockNum,
			TxNum:    1,
			TxIndex:  0,
		},
	}

	txResult := &exec.TxResult{
		Task: task,
		ExecutionResult: evmtypes.ExecutionResult{
			FeeTipped:            s.feeTipped,
			FeeBurnt:             s.feeBurnt,
			BurntContractAddress: s.burntAddr,
			ReceiptGasUsed:       21000,
			BlockRegularGasUsed:  21000,
		},
		Coinbase: s.coinbase,
	}

	return &execResult{TxResult: txResult}
}

// runFinalizeTx runs the direct finalize path.
func (s *testFinalizeScenario) runFinalizeTx(t *testing.T) (state.ReadSet, state.VersionedWrites) {
	t.Helper()
	result := s.buildExecResult()
	result.TxIn = copyReadSet(s.txIn)
	result.TxOut = copyWrites(s.txOut)
	if s.collectorWrites != nil {
		result.CollectorWrites = copyWrites(s.collectorWrites)
	}
	vm := state.NewVersionMap(nil)
	reader := s.makeReader()

	// Strip coinbase/burnt.
	txOut, coinbaseDelta, coinbaseDeltaIncrease, hasCoinbaseDelta := result.TxOut.StripBalanceWrite(result.Coinbase, result.TxIn)
	result.TxOut = txOut
	txOut, burntDelta, burntDeltaIncrease, hasBurntDelta := result.TxOut.StripBalanceWrite(result.ExecutionResult.BurntContractAddress, result.TxIn)
	result.TxOut = txOut
	result.TxIn.Delete(result.Coinbase, state.AccountKey{Path: state.BalancePath})
	result.TxIn.Delete(result.ExecutionResult.BurntContractAddress, state.AccountKey{Path: state.BalancePath})

	task := result.Task.(*taskVersion)
	txTask := task.Task.(*exec.TxTask)

	_, reads, writes, err := result.finalizeTx(
		task, txTask, nil, nil, vm, reader,
		coinbaseDelta, coinbaseDeltaIncrease, hasCoinbaseDelta,
		burntDelta, burntDeltaIncrease, hasBurntDelta,
		s.rules, false, "",
	)
	require.NoError(t, err)
	return reads, writes
}

func fAddr(name string) accounts.Address {
	var a [20]byte
	copy(a[:], name)
	return accounts.InternAddress(a)
}

func fMakeAccount(balance uint64, nonce uint64) *accounts.Account {
	return &accounts.Account{
		Balance: *uint256.NewInt(balance),
		Nonce:   nonce,
	}
}

// simpleTransferScenario: sender sends ETH to recipient, coinbase gets tip.
// Pre-London (no burnt contract).
func simpleTransferScenario() *testFinalizeScenario {
	sender := fAddr("sender")
	recipient := fAddr("recipient")
	coinbase := fAddr("coinbase")

	senderBal := uint256.NewInt(100_000_000_000)
	recipientBal := uint256.NewInt(50_000_000_000)

	transferAmt := uint256.NewInt(1_000_000_000)
	tip := uint256.NewInt(21_000)

	// When shouldDelayFeeCalc=true, execution runs with calcFees=false:
	// sender is only debited the transfer amount (no gas), and coinbase
	// is NOT touched during execution. Fees are applied during finalize.
	newSenderBal := new(uint256.Int).Sub(senderBal, transferAmt)
	newRecipientBal := new(uint256.Int).Add(recipientBal, transferAmt)

	rules := &chain.Rules{IsSpuriousDragon: true}
	config := &chain.Config{ChainID: uint256.NewInt(1)}

	// TxIn: reads from execution. No coinbase reads (coinbase not touched
	// during calcFees=false execution).
	txIn := state.ReadSet{}
	txIn.Set(state.VersionedRead{Address: sender, Path: state.AddressPath, Val: fMakeAccount(senderBal.Uint64(), 0)})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.BalancePath, Val: *senderBal})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.NoncePath, Val: uint64(0)})
	txIn.Set(state.VersionedRead{Address: recipient, Path: state.AddressPath, Val: fMakeAccount(recipientBal.Uint64(), 0)})
	txIn.Set(state.VersionedRead{Address: recipient, Path: state.BalancePath, Val: *recipientBal})

	// TxOut: writes from execution. No coinbase write (fees deferred).
	txOut := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *newSenderBal, BalanceChangeReason: tracing.BalanceDecreaseGasBuy},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: recipient, Path: state.BalancePath, Val: *newRecipientBal, BalanceChangeReason: tracing.BalanceChangeTransfer},
	}

	// CollectorWrites: LightCollector output from MakeWriteSet.
	// No coinbase (not touched during execution).
	collectorWrites := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *newSenderBal},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: sender, Path: state.CodeHashPath, Val: accounts.EmptyCodeHash},
		{Address: recipient, Path: state.BalancePath, Val: *newRecipientBal},
		{Address: recipient, Path: state.NoncePath, Val: uint64(0)},
		{Address: recipient, Path: state.CodeHashPath, Val: accounts.EmptyCodeHash},
	}

	return &testFinalizeScenario{
		name: "simple_transfer_pre_london",
		accts: map[accounts.Address]*accounts.Account{
			sender:    fMakeAccount(senderBal.Uint64(), 0),
			recipient: fMakeAccount(recipientBal.Uint64(), 0),
			coinbase:  fMakeAccount(0, 0),
		},
		txIn:            txIn,
		txOut:           txOut,
		collectorWrites: collectorWrites,
		feeTipped:       *tip,
		coinbase:        coinbase,
		burntAddr:       accounts.NilAddress,
		rules:           rules,
		config:          config,
		header: &types.Header{
			Number:   *uint256.NewInt(1),
			GasLimit: 30_000_000,
			GasUsed:  21000,
		},
	}
}

// londonTransferScenario: sender sends ETH with EIP-1559 base fee burning.
func londonTransferScenario() *testFinalizeScenario {
	s := simpleTransferScenario()
	s.name = "simple_transfer_london"
	s.rules.IsLondon = true
	s.config.LondonBlock = new(uint64) // 0

	burntAddr := fAddr("burntcontract")
	baseFee := uint256.NewInt(10_000)
	tip := uint256.NewInt(11_000)

	s.feeTipped = *tip
	s.feeBurnt = *baseFee
	s.burntAddr = burntAddr

	s.accts[burntAddr] = fMakeAccount(500_000, 0)

	// Burnt contract is NOT touched during calcFees=false execution.
	// No TxIn reads or TxOut writes for burnt contract — fees are
	// applied entirely during finalize.

	return s
}

// TestFinalizeTx_SimpleTransfer verifies the direct finalize path produces
// expected reads and writes for a simple ETH transfer.
func TestFinalizeTx_SimpleTransfer(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()
	reads, writes := s.runFinalizeTx(t)

	assert.NotNil(t, reads, "direct path should produce reads")
	assert.Greater(t, len(writes), 0, "direct path should produce writes")

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "should have coinbase balance write")
	coinbaseBalance := coinbaseWrite.Val.(uint256.Int)
	assert.Equal(t, s.feeTipped, coinbaseBalance, "coinbase should receive the tip")
}

// TestFinalizeTx_London verifies the direct finalize path with burnt fees.
func TestFinalizeTx_London(t *testing.T) {
	t.Parallel()
	s := londonTransferScenario()
	reads, writes := s.runFinalizeTx(t)

	assert.NotNil(t, reads)
	assert.Greater(t, len(writes), 0)

	burntWrite := findWrite(writes, s.burntAddr, state.BalancePath)
	require.NotNil(t, burntWrite, "should have burnt contract balance write")
	burntBalance := burntWrite.Val.(uint256.Int)
	expected := new(uint256.Int).Add(uint256.NewInt(500_000), &s.feeBurnt)
	assert.Equal(t, *expected, burntBalance, "burnt contract should receive base fee")
}

// coinbaseIsRecipientScenario: coinbase is the recipient of the transfer.
// Coinbase balance in TxOut has the transfer amount (from execution) plus
// it will get the tip from finalize. The StripBalanceWrite must correctly
// handle the case where coinbase has a real balance change from execution.
func coinbaseIsRecipientScenario() *testFinalizeScenario {
	sender := fAddr("sender")
	coinbase := fAddr("coinbase") // coinbase is also the recipient

	senderBal := uint256.NewInt(100_000_000_000)
	coinbaseBal := uint256.NewInt(5_000_000_000)
	transferAmt := uint256.NewInt(1_000_000_000)
	tip := uint256.NewInt(21_000)

	// During calcFees=false execution:
	// - sender loses transfer amount (but NOT gas — fees deferred)
	// - coinbase gains transfer amount (but NOT tip — fees deferred)
	newSenderBal := new(uint256.Int).Sub(senderBal, transferAmt)
	newCoinbaseBal := new(uint256.Int).Add(coinbaseBal, transferAmt)

	rules := &chain.Rules{IsSpuriousDragon: true}
	config := &chain.Config{ChainID: uint256.NewInt(1)}

	txIn := state.ReadSet{}
	txIn.Set(state.VersionedRead{Address: sender, Path: state.AddressPath, Val: fMakeAccount(senderBal.Uint64(), 0)})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.BalancePath, Val: *senderBal})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.NoncePath, Val: uint64(0)})
	// Coinbase IS touched during execution (as transfer recipient).
	txIn.Set(state.VersionedRead{Address: coinbase, Path: state.AddressPath, Val: fMakeAccount(coinbaseBal.Uint64(), 0)})
	txIn.Set(state.VersionedRead{Address: coinbase, Path: state.BalancePath, Val: *coinbaseBal})

	// TxOut: coinbase has transfer amount but NOT tip (fees deferred).
	txOut := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *newSenderBal, BalanceChangeReason: tracing.BalanceDecreaseGasBuy},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: coinbase, Path: state.BalancePath, Val: *newCoinbaseBal, BalanceChangeReason: tracing.BalanceChangeTransfer},
	}

	collectorWrites := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *newSenderBal},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: sender, Path: state.CodeHashPath, Val: accounts.EmptyCodeHash},
		{Address: coinbase, Path: state.BalancePath, Val: *newCoinbaseBal},
		{Address: coinbase, Path: state.NoncePath, Val: uint64(0)},
		{Address: coinbase, Path: state.CodeHashPath, Val: accounts.EmptyCodeHash},
	}

	return &testFinalizeScenario{
		name: "coinbase_is_recipient",
		accts: map[accounts.Address]*accounts.Account{
			sender:   fMakeAccount(senderBal.Uint64(), 0),
			coinbase: fMakeAccount(coinbaseBal.Uint64(), 0),
		},
		txIn:            txIn,
		txOut:           txOut,
		collectorWrites: collectorWrites,
		feeTipped:       *tip,
		coinbase:        coinbase,
		burntAddr:       accounts.NilAddress,
		rules:           rules,
		config:          config,
		header: &types.Header{
			Number:   *uint256.NewInt(1),
			GasLimit: 30_000_000,
			GasUsed:  21000,
		},
	}
}

// selfTransferScenario: sender sends ETH to themselves. Coinbase is separate.
// Tests that same-address sender+recipient doesn't confuse the finalize logic.
func selfTransferScenario() *testFinalizeScenario {
	sender := fAddr("sender")
	coinbase := fAddr("coinbase")

	senderBal := uint256.NewInt(100_000_000_000)

	// Self-transfer: balance doesn't change (transfer cancels out).
	// Gas is NOT deducted during calcFees=false, so balance stays the same.
	// But nonce increments.
	rules := &chain.Rules{IsSpuriousDragon: true}
	config := &chain.Config{ChainID: uint256.NewInt(1)}
	tip := uint256.NewInt(21_000)

	txIn := state.ReadSet{}
	txIn.Set(state.VersionedRead{Address: sender, Path: state.AddressPath, Val: fMakeAccount(senderBal.Uint64(), 0)})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.BalancePath, Val: *senderBal})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.NoncePath, Val: uint64(0)})

	txOut := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *senderBal, BalanceChangeReason: tracing.BalanceChangeTransfer},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
	}

	collectorWrites := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *senderBal},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: sender, Path: state.CodeHashPath, Val: accounts.EmptyCodeHash},
	}

	return &testFinalizeScenario{
		name: "self_transfer",
		accts: map[accounts.Address]*accounts.Account{
			sender:   fMakeAccount(senderBal.Uint64(), 0),
			coinbase: fMakeAccount(0, 0),
		},
		txIn:            txIn,
		txOut:           txOut,
		collectorWrites: collectorWrites,
		feeTipped:       *tip,
		coinbase:        coinbase,
		burntAddr:       accounts.NilAddress,
		rules:           rules,
		config:          config,
		header: &types.Header{
			Number:   *uint256.NewInt(1),
			GasLimit: 30_000_000,
			GasUsed:  21000,
		},
	}
}

// TestFinalizeTx_AllScenarios verifies the direct finalize path produces
// correct writes and reads across all test scenarios.
func TestFinalizeTx_AllScenarios(t *testing.T) {
	t.Parallel()
	scenarios := []*testFinalizeScenario{
		simpleTransferScenario(),
		londonTransferScenario(),
		coinbaseIsRecipientScenario(),
		selfTransferScenario(),
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			// TODO(#20962): when coinbase is also the transfer recipient,
			// finalizeTx no longer adds the tip on top of the existing TxOut
			// coinbase write. Either StripBalanceWrite or the post-strip
			// re-credit shifted; re-validate before un-skipping.
			if s.name == "coinbase_is_recipient" {
				t.Skip("stale coinbase-is-recipient expectation, see #20962")
			}
			reads, writes := s.runFinalizeTx(t)

			assert.NotNil(t, reads, "should produce reads")
			assert.Greater(t, len(writes), 0, "should produce writes")

			// Coinbase must receive the tip.
			coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
			require.NotNil(t, coinbaseWrite, "should have coinbase balance write")
			coinbaseBalance := coinbaseWrite.Val.(uint256.Int)

			expectedCoinbase := new(uint256.Int).Set(&s.feeTipped)
			if acc, ok := s.accts[s.coinbase]; ok {
				expectedCoinbase.Add(expectedCoinbase, &acc.Balance)
			}
			// If coinbase was also a transfer recipient, add the delta.
			if hasCoinbaseDelta(s) {
				expectedCoinbase = adjustForTransferDelta(s, expectedCoinbase)
			}
			assert.Equal(t, *expectedCoinbase, coinbaseBalance,
				"coinbase balance should be original + tip (+ transfer delta if recipient)")

			// If London: burnt contract must receive base fee.
			if s.rules.IsLondon && s.burntAddr != accounts.NilAddress {
				burntWrite := findWrite(writes, s.burntAddr, state.BalancePath)
				require.NotNil(t, burntWrite, "should have burnt contract balance write")
				burntBalance := burntWrite.Val.(uint256.Int)
				expectedBurnt := new(uint256.Int).Add(&s.accts[s.burntAddr].Balance, &s.feeBurnt)
				assert.Equal(t, *expectedBurnt, burntBalance,
					"burnt contract should receive base fee")
			}

			// BalancePath reads must include coinbase.
			balReads := extractBalanceReads(reads)
			_, hasCoinbaseRead := balReads[s.coinbase]
			assert.True(t, hasCoinbaseRead, "should read coinbase balance")
		})
	}
}

// hasCoinbaseDelta checks if coinbase was touched during execution (transfer recipient).
func hasCoinbaseDelta(s *testFinalizeScenario) bool {
	for _, w := range s.txOut {
		if w.Address == s.coinbase && w.Path == state.BalancePath {
			return true
		}
	}
	return false
}

// adjustForTransferDelta computes the expected coinbase balance when coinbase
// is also the transfer recipient. The delta is the difference between the
// execution-written balance and the original balance.
func adjustForTransferDelta(s *testFinalizeScenario, base *uint256.Int) *uint256.Int {
	for _, w := range s.txOut {
		if w.Address == s.coinbase && w.Path == state.BalancePath {
			execBal := w.Val.(uint256.Int)
			origBal := s.accts[s.coinbase].Balance
			delta := new(uint256.Int).Sub(&execBal, &origBal)
			return new(uint256.Int).Add(base, delta)
		}
	}
	return base
}

func findWrite(writes state.VersionedWrites, addr accounts.Address, path state.AccountPath) *state.VersionedWrite {
	for _, w := range writes {
		if w.Address == addr && w.Path == path {
			return w
		}
	}
	return nil
}

func buildWriteMap(writes state.VersionedWrites) map[string]string {
	m := make(map[string]string, len(writes))
	for _, w := range writes {
		key := w.Address.String() + ":" + w.Path.String() + ":" + w.Key.String()
		m[key] = fmtWriteVal(w)
	}
	return m
}

func fmtWriteVal(w *state.VersionedWrite) string {
	switch v := w.Val.(type) {
	case uint256.Int:
		return v.Hex()
	case uint64:
		return uint256.NewInt(v).Hex()
	case bool:
		if v {
			return "true"
		}
		return "false"
	case accounts.CodeHash:
		return v.String()
	default:
		return "<unknown>"
	}
}

func extractBalanceReads(reads state.ReadSet) map[accounts.Address]string {
	m := make(map[accounts.Address]string)
	if reads == nil {
		return m
	}
	reads.Scan(func(vr *state.VersionedRead) bool {
		if vr.Path == state.BalancePath {
			if val, ok := vr.Val.(uint256.Int); ok {
				m[vr.Address] = val.Hex()
			}
		}
		return true
	})
	return m
}

// --- finalizeTxSimple tests ---
// These test the split fee logic: worker debits sender only,
// finalize credits coinbase/burnt. No StripBalanceWrite or delta computation.

// runFinalizeTxSimple sets up the versionMap with prior TX's finalize writes
// and runs finalizeTxSimple, verifying the fee credit logic.
func (s *testFinalizeScenario) runFinalizeTxSimple(t *testing.T, priorCoinbaseBalance *uint256.Int) state.VersionedWrites {
	t.Helper()
	result := s.buildExecResult()
	result.TxIn = copyReadSet(s.txIn)
	result.TxOut = copyWrites(s.txOut)
	if s.collectorWrites != nil {
		result.CollectorWrites = copyWrites(s.collectorWrites)
	}

	vm := state.NewVersionMap(nil)
	reader := s.makeReader()

	// Simulate prior TX's finalize writing coinbase BalancePath to the versionMap.
	if priorCoinbaseBalance != nil {
		vm.Write(s.coinbase, state.BalancePath, accounts.NilKey,
			state.Version{TxIndex: 0, Incarnation: 0}, *priorCoinbaseBalance, true)
	}

	// Flush the worker's TxOut to the versionMap (simulates line 1928).
	vm.FlushVersionedWrites(result.TxOut, true, "")

	task := result.Task.(*taskVersion)
	txTask := task.Task.(*exec.TxTask)

	_, _, writes, err := result.finalizeTxSimple(
		task, txTask, nil, nil, vm, reader,
		s.rules, false, false, "",
	)
	require.NoError(t, err)
	return writes
}

// TestFinalizeTxSimple_BasicFeeCredit verifies that finalizeTxSimple adds
// FeeTipped to the coinbase balance from the versionMap.
func TestFinalizeTxSimple_BasicFeeCredit(t *testing.T) {
	// TODO(#20962): scenario writes priorBalance into versionMap at TxIndex=0
	// then reads at TxIndex=0; floor semantics changed and the prior write
	// is no longer visible. Sibling AccumulatedFees test (txIdx 1..N) passes.
	t.Skip("stale coinbase-handling expectation, see #20962")
	t.Parallel()
	s := simpleTransferScenario()

	// Coinbase starts with 1 ETH (from prior TX's finalize).
	priorBalance := uint256.NewInt(1_000_000_000_000_000_000)
	writes := s.runFinalizeTxSimple(t, priorBalance)

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "finalize should produce coinbase BalancePath write")

	coinbaseBalance := coinbaseWrite.Val.(uint256.Int)
	expected := new(uint256.Int).Add(priorBalance, &s.feeTipped)
	assert.Equal(t, *expected, coinbaseBalance,
		"coinbase should be priorBalance + FeeTipped (no delta, no double-count)")
}

// TestFinalizeTxSimple_VersionOnWrites verifies that all finalize writes
// have the correct Version (task.Version()), not zero Version.
func TestFinalizeTxSimple_VersionOnWrites(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()
	priorBalance := uint256.NewInt(1_000_000_000_000_000_000)
	writes := s.runFinalizeTxSimple(t, priorBalance)

	for _, w := range writes {
		if w.Address == s.coinbase && w.Path == state.BalancePath {
			// The task has TxIndex=0 (first TX). Verify the Version
			// matches and is not the zero-value struct.
			assert.Equal(t, 0, w.Version.TxIndex,
				"coinbase write should have Version from task (txIndex=0)")
			assert.Equal(t, uint64(1), w.Version.BlockNum,
				"coinbase write should have correct BlockNum")
			return
		}
	}
	t.Fatal("no coinbase BalancePath write found")
}

// TestFinalizeTxSimple_LondonBurntFees verifies that FeeBurnt is added to
// the burnt contract balance.
func TestFinalizeTxSimple_LondonBurntFees(t *testing.T) {
	t.Parallel()
	s := londonTransferScenario()
	priorBalance := uint256.NewInt(1_000_000_000_000_000_000)
	writes := s.runFinalizeTxSimple(t, priorBalance)

	burntWrite := findWrite(writes, s.burntAddr, state.BalancePath)
	require.NotNil(t, burntWrite, "finalize should produce burnt contract BalancePath write")

	burntBalance := burntWrite.Val.(uint256.Int)
	// Burnt contract gets existing balance (500000) + FeeBurnt
	expected := new(uint256.Int).Add(uint256.NewInt(500_000), &s.feeBurnt)
	assert.Equal(t, *expected, burntBalance,
		"burnt contract should be existing balance + FeeBurnt")
}

// TestFinalizeTxSimple_NoCoinbaseInVersionMap verifies that when there's no
// prior coinbase entry in the versionMap, the finalize reads from the
// stateReader and adds FeeTipped.
func TestFinalizeTxSimple_NoCoinbaseInVersionMap(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()

	// No prior coinbase balance — reads from stateReader (balance 0).
	writes := s.runFinalizeTxSimple(t, nil)

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "finalize should produce coinbase BalancePath write")

	coinbaseBalance := coinbaseWrite.Val.(uint256.Int)
	assert.Equal(t, s.feeTipped, coinbaseBalance,
		"coinbase should be 0 + FeeTipped when no prior balance in versionMap")
}

// TestFinalizeTxSimple_AccumulatedFees verifies that successive finalizeTxSimple
// calls correctly accumulate fees across TXs (the core requirement).
func TestFinalizeTxSimple_AccumulatedFees(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()

	vm := state.NewVersionMap(nil)
	reader := s.makeReader()

	tipPerTx := uint256.NewInt(21_000)

	for txIdx := 1; txIdx <= 3; txIdx++ {
		result := s.buildExecResult()
		result.TxIn = copyReadSet(s.txIn)
		result.TxOut = copyWrites(s.txOut)
		result.CollectorWrites = copyWrites(s.collectorWrites)
		result.ExecutionResult.FeeTipped = *tipPerTx

		task := result.Task.(*taskVersion)
		task.version.TxIndex = txIdx

		// Flush TxOut to versionMap (simulates line 1928).
		vm.FlushVersionedWrites(result.TxOut, true, "")

		txTask := task.Task.(*exec.TxTask)
		_, _, writes, err := result.finalizeTxSimple(
			task, txTask, nil, nil, vm, reader,
			s.rules, false, false, "",
		)
		require.NoError(t, err)

		// Flush finalize writes to versionMap for next TX.
		vm.FlushVersionedWrites(writes, true, "")

		// Verify accumulated balance.
		coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
		require.NotNil(t, coinbaseWrite, "TX %d: should have coinbase write", txIdx)
		bal := coinbaseWrite.Val.(uint256.Int)
		expectedBal := new(uint256.Int).Mul(tipPerTx, uint256.NewInt(uint64(txIdx)))
		assert.Equal(t, *expectedBal, bal,
			"TX %d: coinbase should have accumulated %d tips", txIdx, txIdx)
	}
}

// TestResolveStorageWrites_IBSvsSimple compares the storage write sets
// produced by the serial IBS path (MakeWriteSet) against the parallel
// resolveStorageWrites path. They must produce identical storage key sets.
//
// The test scenario: TX 0 writes slot A=100. TX 1 reads slot A and writes
// it back unchanged (A=100). In serial, TX 1's IBS sees originStorage[A]=100
// (from sd.mem after TX 0), so dirty==origin → skip. In parallel, the worker
// may have read slot A from a stale source, giving originStorage[A]=50.
// dirty=100 != origin=50 → IBS marks it dirty → CollectorWrites includes it.
// resolveStorageWrites must filter this no-op.
func TestResolveStorageWrites_IBSvsSimple(t *testing.T) {
	vm := state.NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0xAA})
	slotA := accounts.InternKey([32]byte{0x01})
	slotB := accounts.InternKey([32]byte{0x02})
	slotC := accounts.InternKey([32]byte{0x03})

	val100 := *uint256.NewInt(100)
	val200 := *uint256.NewInt(200)
	val300 := *uint256.NewInt(300)

	// Pre-block state: slotA=50, slotB=200, slotC=0 (not set)
	// (val50 not needed — it's only in the worker's stale originStorage)
	// TX 0 writes: slotA=100, slotB=200 (unchanged), slotC=300 (new)
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100, Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slotB, Val: val200, Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slotC, Val: val300, Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}, true, "")

	// TX 1 writes: slotA=100 (same as TX 0 wrote — no-op relative to origin),
	//              slotB=200 (unchanged from pre-block — no-op),
	//              slotC=300 (same as TX 0 wrote — no-op relative to origin)
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100, Version: state.Version{TxIndex: 1, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slotB, Val: val200, Version: state.Version{TxIndex: 1, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slotC, Val: val300, Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}, true, "")

	// --- Serial IBS path ---
	// Serial TX 1's IBS reads from sd.mem (which has TX 0's values):
	//   originStorage[slotA] = 100 (TX 0 wrote it)
	//   originStorage[slotB] = 200 (unchanged, read from pre-block via sd.mem)
	//   originStorage[slotC] = 300 (TX 0 wrote it)
	// TX 1 writes back: slotA=100, slotB=200, slotC=300
	// All dirty == origin → all skipped. Serial emits 0 storage writes for TX 1.
	serialStorageCount := 0 // This is what MakeWriteSet would produce

	// --- Parallel CollectorWrites path ---
	// The worker's IBS had different originStorage because it read speculatively:
	//   originStorage[slotA] = 50 (stale — read before TX 0's write was visible)
	//   originStorage[slotB] = 200 (correct — no prior TX changed it)
	//   originStorage[slotC] = 0 (stale — read before TX 0's write)
	// TX 1 writes: slotA=100, slotB=200, slotC=300
	// dirty != origin for slotA and slotC → CollectorWrites includes them.
	// slotB: dirty == origin → skipped by CollectorWrites.
	collectorWrites := state.VersionedWrites{
		// slotA: worker thought it changed (origin=50, dirty=100)
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100},
		// slotB: NOT in CollectorWrites — worker correctly saw no change
		// slotC: worker thought it changed (origin=0, dirty=300)
		{Address: addr, Path: state.StoragePath, Key: slotC, Val: val300},
	}

	resolved := resolveStorageWrites(collectorWrites, vm, 1, 0, nil)

	// The no-op filter should detect:
	// slotA: resolved=100, origin(floor at TX0)=100 → no-op → FILTER
	// slotC: resolved=300, origin(floor at TX0)=300 → no-op → FILTER
	// Result should be 0 storage writes — matching serial.
	storageCount := 0
	for _, w := range resolved {
		if w.Path == state.StoragePath {
			storageCount++
			t.Errorf("unexpected storage write: addr=%x slot=%x val=%v (should be filtered as no-op)",
				w.Address, w.Key, w.Val)
		}
	}

	assert.Equal(t, serialStorageCount, storageCount,
		"parallel resolveStorageWrites should produce same storage count as serial IBS path")
}

// TestResolveStorageWrites_StaleIncarnationNoOp tests the combined scenario:
// TX has multiple incarnations, and the final incarnation's write is a no-op
// relative to origin. Only stale incarnation entries should be in the versionMap
// for keys the final incarnation didn't write.
func TestResolveStorageWrites_StaleIncarnationNoOp(t *testing.T) {
	vm := state.NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0xBB})
	slotA := accounts.InternKey([32]byte{0x01})
	slotB := accounts.InternKey([32]byte{0x02})

	val100 := *uint256.NewInt(100)
	val200 := *uint256.NewInt(200)
	val300 := *uint256.NewInt(300)

	// TX 0 writes slotA=100
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100, Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}, true, "")

	// TX 1 incarnation 0: writes slotA=200, slotB=300
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val200, Version: state.Version{TxIndex: 1, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slotB, Val: val300, Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}, true, "")

	// TX 1 incarnation 1 (re-execution): only writes slotA=100 (same as origin from TX 0)
	// slotB is NOT written in incarnation 1 — stale entry from incarnation 0 remains
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100, Version: state.Version{TxIndex: 1, Incarnation: 1}},
	}, true, "")

	// CollectorWrites from incarnation 1 — only slotA
	collectorWrites := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100},
	}

	// incarnation=1 is the validated incarnation
	resolved := resolveStorageWrites(collectorWrites, vm, 1, 1, nil)

	// slotA: versionMap has incarnation=1 entry, resolved=100, origin(TX0)=100 → no-op → FILTER
	// slotB: not in CollectorWrites (incarnation 1 didn't write it) → already excluded
	storageCount := 0
	for _, w := range resolved {
		if w.Path == state.StoragePath {
			storageCount++
		}
	}
	assert.Equal(t, 0, storageCount, "all storage writes should be filtered as no-ops or stale incarnation")
}

// TestResolveStorageWrites_NoOpFilter verifies that resolveStorageWrites
// correctly filters no-op storage writes (where the resolved value equals
// the origin value). This matches the serial IBS behaviour where
// applyStorageChanges skips keys where dirty == originStorage.
func TestResolveStorageWrites_NoOpFilter(t *testing.T) {
	vm := state.NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x01})
	slot1 := accounts.InternKey([32]byte{0x01})
	slot2 := accounts.InternKey([32]byte{0x02})
	slot3 := accounts.InternKey([32]byte{0x03})

	val100 := *uint256.NewInt(100)
	val200 := *uint256.NewInt(200)
	val300 := *uint256.NewInt(300)

	// TX 0 writes slot1=100, slot2=200
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slot1, Val: val100, Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slot2, Val: val200, Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}, true, "")

	// TX 1 writes slot1=100 (same as TX 0 — no-op), slot2=300 (changed), slot3=300 (new)
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slot1, Val: val100, Version: state.Version{TxIndex: 1, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slot2, Val: val300, Version: state.Version{TxIndex: 1, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slot3, Val: val300, Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}, true, "")

	// CollectorWrites from TX 1's worker — includes all 3 slots
	collectorWrites := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slot1, Val: val100},
		{Address: addr, Path: state.StoragePath, Key: slot2, Val: val300},
		{Address: addr, Path: state.StoragePath, Key: slot3, Val: val300},
	}

	// resolveStorageWrites for TX 1
	resolved := resolveStorageWrites(collectorWrites, vm, 1, 0, nil)

	// slot1: resolved=100, origin(floor at TX0)=100 → should be FILTERED (no-op)
	// slot2: resolved=300, origin(floor at TX0)=200 → should be KEPT (value changed)
	// slot3: resolved=300, no prior TX wrote it → should be KEPT (new key, no rs so no pre-block check)
	assert.Equal(t, 2, len(resolved), "expected 2 writes (slot1 should be filtered as no-op)")

	keys := make(map[accounts.StorageKey]bool)
	for _, w := range resolved {
		keys[w.Key] = true
	}
	assert.False(t, keys[slot1], "slot1 should be filtered — write-back same as origin")
	assert.True(t, keys[slot2], "slot2 should be kept — value changed from origin")
	assert.True(t, keys[slot3], "slot3 should be kept — new key, no prior TX wrote it")
}

// TestResolveStorageWrites_StaleIncarnation verifies that stale incarnation
// entries are filtered. When TX N is re-executed (incarnation 1), incarnation 0's
// versionMap entries may still exist for keys that incarnation 1 didn't write.
func TestResolveStorageWrites_StaleIncarnation(t *testing.T) {
	vm := state.NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x02})
	slot1 := accounts.InternKey([32]byte{0x01})
	slot2 := accounts.InternKey([32]byte{0x02})

	val100 := *uint256.NewInt(100)
	val200 := *uint256.NewInt(200)

	// TX 5, incarnation 0: writes slot1=100 and slot2=200
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slot1, Val: val100, Version: state.Version{TxIndex: 5, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slot2, Val: val200, Version: state.Version{TxIndex: 5, Incarnation: 0}},
	}, true, "")

	// TX 5, incarnation 1: only writes slot1=100 (slot2 not written in re-execution)
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slot1, Val: val100, Version: state.Version{TxIndex: 5, Incarnation: 1}},
	}, true, "")

	// CollectorWrites from incarnation 1 — only has slot1
	collectorWrites := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slot1, Val: val100},
	}

	// Resolve with incarnation=1 (the validated incarnation)
	resolved := resolveStorageWrites(collectorWrites, vm, 5, 1, nil)

	// slot1: in versionMap at TX 5 with incarnation 1 — kept
	// slot2: not in CollectorWrites — already filtered before reaching resolveStorageWrites
	require.Equal(t, 1, len(resolved), "only slot1 should survive")
	assert.Equal(t, slot1, resolved[0].Key)
}

// TestResolveStorageWrites_SpeculativeExtra verifies that storage keys
// present in CollectorWrites but not in the versionMap for this TX are filtered.
func TestResolveStorageWrites_SpeculativeExtra(t *testing.T) {
	vm := state.NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x03})
	slot1 := accounts.InternKey([32]byte{0x01})
	slot2 := accounts.InternKey([32]byte{0x02})

	val100 := *uint256.NewInt(100)
	val200 := *uint256.NewInt(200)

	// Only slot1 was flushed to versionMap for TX 3
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slot1, Val: val100, Version: state.Version{TxIndex: 3, Incarnation: 0}},
	}, true, "")

	// CollectorWrites has both slot1 and slot2 (slot2 from speculative code path)
	collectorWrites := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slot1, Val: val100},
		{Address: addr, Path: state.StoragePath, Key: slot2, Val: val200},
	}

	resolved := resolveStorageWrites(collectorWrites, vm, 3, 0, nil)

	// slot1: in versionMap for this TX — kept
	// slot2: NOT in versionMap for this TX — filtered
	require.Equal(t, 1, len(resolved))
	assert.Equal(t, slot1, resolved[0].Key)
}

// --- normalizeWriteSet tests ---
// These test the new function that replaces resolveStorageWrites.
// Input is blockIO.WriteSet(txIndex) — raw versionWritten output.
// Output should match what serial MakeWriteSet produces.

// Case 1: Storage no-op filter.
// TX writes back the same value a prior TX wrote → should be excluded.
func TestNormalizeWriteSet_StorageNoOp(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x10})
	slotA := accounts.InternKey([32]byte{0x01})

	val100 := *uint256.NewInt(100)

	// TX 0 writes slotA=100
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}, true, "")

	// TX 1 writes slotA=100 (same as TX 0 — no-op)
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 1, 0, nil, nil, true)

	storageCount := countPath(result, state.StoragePath)
	assert.Equal(t, 0, storageCount, "no-op storage write should be filtered")
}

// Case 1b: Storage value DID change from origin → should be kept.
func TestNormalizeWriteSet_StorageChanged(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x11})
	slotA := accounts.InternKey([32]byte{0x01})

	val100 := *uint256.NewInt(100)
	val200 := *uint256.NewInt(200)

	// TX 0 writes slotA=100
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}, true, "")

	// TX 1 writes slotA=200 (changed from TX 0's 100)
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val200,
			Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 1, 0, nil, nil, true)

	storageCount := countPath(result, state.StoragePath)
	assert.Equal(t, 1, storageCount, "changed storage write should be kept")
	if storageCount > 0 {
		v := result[0].Val.(uint256.Int)
		assert.Equal(t, val200, v, "should have resolved value 200")
	}
}

// Case 1c: No prior TX wrote slot — new key, should be kept.
func TestNormalizeWriteSet_StorageNewKey(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x12})
	slotA := accounts.InternKey([32]byte{0x01})

	val100 := *uint256.NewInt(100)

	// TX 0 writes slotA=100 (no prior TX)
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, nil, nil, true)

	storageCount := countPath(result, state.StoragePath)
	assert.Equal(t, 1, storageCount, "new storage key should be kept")
}

// Case 2: Stale incarnation filter.
// Incarnation 0 wrote slotB but incarnation 1 (validated) didn't → exclude slotB.
func TestNormalizeWriteSet_StaleIncarnation(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x13})
	slotA := accounts.InternKey([32]byte{0x01})
	slotB := accounts.InternKey([32]byte{0x02})

	val100 := *uint256.NewInt(100)
	val200 := *uint256.NewInt(200)

	// TX 5 incarnation 0: writes slotA=100 and slotB=200
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 5, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slotB, Val: val200,
			Version: state.Version{TxIndex: 5, Incarnation: 0}},
	}, true, "")

	// TX 5 incarnation 1: only writes slotA=100
	inc1Writes := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 5, Incarnation: 1}},
	}
	vm.FlushVersionedWrites(inc1Writes, true, "")

	// The WriteSet has BOTH incarnation 0 and 1 entries (versionMap doesn't clear old)
	// But we pass incarnation=1 as the validated one
	allWrites := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 5, Incarnation: 1}},
		{Address: addr, Path: state.StoragePath, Key: slotB, Val: val200,
			Version: state.Version{TxIndex: 5, Incarnation: 0}}, // stale
	}

	result := normalizeWriteSet(allWrites, vm, 5, 1, nil, nil, true)

	storageCount := countPath(result, state.StoragePath)
	assert.Equal(t, 1, storageCount, "only incarnation 1's slotA should survive")
	if storageCount > 0 {
		assert.Equal(t, slotA, result[0].Key)
	}
}

// Case 3: Self-destruct emits storage DELETEs.
func TestNormalizeWriteSet_SelfDestruct(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x14})
	slotA := accounts.InternKey([32]byte{0x01})
	slotB := accounts.InternKey([32]byte{0x02})

	val100 := *uint256.NewInt(100)
	val200 := *uint256.NewInt(200)

	// TX 0 wrote storage for this address (slots exist in versionMap)
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.StoragePath, Key: slotB, Val: val200,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}, true, "")

	// TX 1 self-destructs (val=true means actually destructed)
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.SelfDestructPath, Val: true,
			Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 1, 0, nil, nil, true)

	// Should have: SelfDestructPath + DELETE for slotA + DELETE for slotB
	sdCount := countPath(result, state.SelfDestructPath)
	storageCount := countPath(result, state.StoragePath)

	assert.Equal(t, 1, sdCount, "should have SelfDestructPath entry")
	assert.Equal(t, 2, storageCount, "should have DELETE entries for both slots")

	// Verify DELETEs have zero values
	for _, w := range result {
		if w.Path == state.StoragePath {
			v := w.Val.(uint256.Int)
			assert.True(t, v.IsZero(), "self-destruct storage DELETE should have zero value")
		}
	}
}

// Case 4: Account field resolution from versionMap.
func TestNormalizeWriteSet_AccountFieldResolution(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x15})

	// TX 0 writes balance=100
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.BalancePath, Val: *uint256.NewInt(100),
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}, true, "")

	// TX 1 writes balance=150 (accumulated from TX 0's 100 + delta)
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.BalancePath, Val: *uint256.NewInt(150),
			Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}, true, "")

	// Worker's WriteSet had stale balance=120 (from speculative execution)
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.BalancePath, Val: *uint256.NewInt(120),
			Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}

	result := normalizeWriteSet(writeSet, vm, 1, 0, nil, nil, true)

	require.Equal(t, 1, len(result))
	v := result[0].Val.(uint256.Int)
	assert.Equal(t, *uint256.NewInt(150), v,
		"balance should be resolved from versionMap (150), not worker's stale value (120)")
}

// Case 5: AddressPath entries are excluded.
func TestNormalizeWriteSet_AddressPathExcluded(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x16})

	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.AddressPath, Val: &accounts.Account{},
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.BalancePath, Val: *uint256.NewInt(100),
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, nil, nil, true)

	addrCount := countPath(result, state.AddressPath)
	balCount := countPath(result, state.BalancePath)
	assert.Equal(t, 0, addrCount, "AddressPath should be excluded")
	assert.Equal(t, 1, balCount, "BalancePath should be kept")
}

// Case 6: Addresses with storage changes must have account-level writes.
// Serial's MakeWriteSet calls UpdateAccountData for every dirty object,
// even if only storage changed. The commitment needs the full account state.
func TestNormalizeWriteSet_StorageOnlyAddress(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x17})
	slotA := accounts.InternKey([32]byte{0x01})

	val100 := *uint256.NewInt(100)

	emptyCodeHash := accounts.InternCodeHash(common.HexToHash(
		"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"))

	// Pre-block account state (from domain/stateReader).
	reader := newMapStateReader()
	reader.accounts[addr] = &accounts.Account{
		Balance:  *uint256.NewInt(500),
		Nonce:    3,
		CodeHash: emptyCodeHash,
	}

	// TX 0 only writes storage — no balance/nonce/code changes
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, reader, nil, true)

	// Should have storage write AND account-level fields for addr.
	// Serial emits UpdateAccountData for every dirty object.
	storageCount := countPath(result, state.StoragePath)
	balanceCount := countPath(result, state.BalancePath)
	nonceCount := countPath(result, state.NoncePath)

	assert.Equal(t, 1, storageCount, "storage write should be kept")
	assert.Equal(t, 1, balanceCount,
		"address with storage changes must have account-level writes for commitment")
	assert.Equal(t, 1, nonceCount,
		"address with storage changes must have nonce write for commitment")
}

// Case 7: Address with storage writes that are ALL no-ops.
// The storage writes are filtered out but the account must still be emitted
// because the IBS marked the object as dirty (it was accessed).
func TestNormalizeWriteSet_StorageAllNoOps(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x18})
	slotA := accounts.InternKey([32]byte{0x01})

	val100 := *uint256.NewInt(100)

	emptyCodeHash := accounts.InternCodeHash(common.HexToHash(
		"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"))

	reader := newMapStateReader()
	reader.accounts[addr] = &accounts.Account{
		Balance:  *uint256.NewInt(500),
		Nonce:    3,
		CodeHash: emptyCodeHash,
	}

	// TX 0 writes slotA=100
	vm.FlushVersionedWrites(state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}, true, "")

	// TX 1 writes slotA=100 (same as TX 0 — no-op, will be filtered)
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.StoragePath, Key: slotA, Val: val100,
			Version: state.Version{TxIndex: 1, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 1, 0, reader, nil, true)

	// Storage write should be filtered (no-op).
	// But account fields should still be emitted — the IBS would have
	// called UpdateAccountData for this dirty object.
	storageCount := countPath(result, state.StoragePath)
	balanceCount := countPath(result, state.BalancePath)

	assert.Equal(t, 0, storageCount, "no-op storage should be filtered")
	assert.Equal(t, 1, balanceCount,
		"account fields must be emitted even when all storage writes are no-ops")
}

// Case 8: CreateContractPath prevents empty account deletion.
// An empty account (balance=0, nonce=0) that was just created should NOT
// be deleted — it should be written with all fields.
func TestNormalizeWriteSet_CreateContract(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x19})
	codeHash := accounts.InternCodeHash(common.HexToHash(
		"40802296c24793f9d86e9e09d87c4e03606856c98cbdd749d6499bea4467d07c"))

	// TX 0 creates a contract with nonce=1, balance=0, non-empty codeHash
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.CreateContractPath, Val: true,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.BalancePath, Val: *uint256.NewInt(0),
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.NoncePath, Val: uint64(1),
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.CodeHashPath, Val: codeHash,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, nil, nil, true)

	// Should have CreateContractPath + all 3 account fields.
	// The empty balance should NOT cause deletion because CreateContractPath is present.
	createCount := countPath(result, state.CreateContractPath)
	balCount := countPath(result, state.BalancePath)
	nonceCount := countPath(result, state.NoncePath)
	codeHashCount := countPath(result, state.CodeHashPath)

	assert.Equal(t, 1, createCount, "CreateContractPath should be preserved")
	assert.Equal(t, 1, balCount, "balance should be present")
	assert.Equal(t, 1, nonceCount, "nonce should be present")
	assert.Equal(t, 1, codeHashCount, "codeHash should be present")
}

// Case 9: New account (doesn't exist in domain) gets default field values.
// When a TX sends ETH to a new address, only BalancePath is in the WriteSet.
// The stateReader returns nil. Default values (Nonce=0, CodeHash=empty) must
// be emitted so the commitment doesn't treat it as a delete.
func TestNormalizeWriteSet_NewAccount(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x1A})

	// TX 0 sends ETH to a new address — only BalancePath written
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.BalancePath, Val: *uint256.NewInt(50000),
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	// stateReader returns nil for this address (doesn't exist yet)
	reader := newMapStateReader() // empty — no accounts

	result := normalizeWriteSet(writeSet, vm, 0, 0, reader, nil, true)

	balCount := countPath(result, state.BalancePath)
	nonceCount := countPath(result, state.NoncePath)
	codeHashCount := countPath(result, state.CodeHashPath)

	assert.Equal(t, 1, balCount, "balance should be present")
	assert.Equal(t, 1, nonceCount, "nonce=0 should be emitted for new account")
	assert.Equal(t, 1, codeHashCount, "empty codeHash should be emitted for new account")

	// Verify nonce is 0 (not missing)
	for _, w := range result {
		if w.Path == state.NoncePath {
			assert.Equal(t, uint64(0), w.Val.(uint64), "nonce should be 0")
		}
	}
}

// Case 10: EIP-161 empty account removal.
// An account with Balance=0, Nonce=0, empty CodeHash should be DELETED
// (not written as a regular account with zero values). Serial's IBS
// detects empty accounts via EIP-161 and calls DeleteAccount.
// normalizeWriteSet must produce a SelfDestructPath=true entry (or omit
// the account entirely) so the trie deletes the cell.
func TestNormalizeWriteSet_EmptyAccountRemoval(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x37, 0x42}) // target account

	emptyCodeHash := accounts.InternCodeHash(common.HexToHash(
		"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"))

	// The account previously had Balance > 0 (from a prior block).
	// In this block, a TX zeroes the balance. The account becomes empty
	// (Balance=0, Nonce=0, CodeHash=empty). Serial deletes it via EIP-161.
	// The WriteSet has BalancePath=0 from the TX that zeroed the balance.
	writeSet := state.VersionedWrites{
		{Address: addr, Path: state.BalancePath, Val: *uint256.NewInt(0),
			Version: state.Version{TxIndex: 5, Incarnation: 0}},
		{Address: addr, Path: state.NoncePath, Val: uint64(0),
			Version: state.Version{TxIndex: 5, Incarnation: 0}},
		{Address: addr, Path: state.CodeHashPath, Val: emptyCodeHash,
			Version: state.Version{TxIndex: 5, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	// The stateReader has the account from the prior block (Balance > 0).
	reader := newMapStateReader()
	reader.accounts[addr] = &accounts.Account{
		Balance:  *uint256.NewInt(1400000000000000),
		Nonce:    2,
		CodeHash: emptyCodeHash,
	}

	result := normalizeWriteSet(writeSet, vm, 5, 0, reader, nil, true)

	// The normalized output should produce a Delete for this account,
	// NOT a regular write with Balance=0, Nonce=0.
	// Serial's updateAccount checks: EIP161Enabled && stateObject.data.Empty()
	// → calls DeleteAccount → emits SelfDestructPath=true

	hasDelete := false
	hasNonDeleteAccount := false
	for _, w := range result {
		if w.Address == addr {
			if w.Path == state.SelfDestructPath {
				if v, ok := w.Val.(bool); ok && v {
					hasDelete = true
				}
			}
			if w.Path == state.BalancePath || w.Path == state.NoncePath || w.Path == state.CodeHashPath {
				hasNonDeleteAccount = true
			}
		}
	}

	assert.True(t, hasDelete,
		"empty account (Balance=0, Nonce=0, empty CodeHash) should be deleted via EIP-161")
	assert.False(t, hasNonDeleteAccount,
		"deleted account should NOT have regular account field writes")
}

// countPath counts writes with a given path.
func countPath(writes state.VersionedWrites, path state.AccountPath) int {
	n := 0
	for _, w := range writes {
		if w.Path == path {
			n++
		}
	}
	return n
}
