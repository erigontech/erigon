package stagedsync

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func (r *mapStateReader) ReadAccountIncarnation(addr accounts.Address) (uint64, error) {
	if a, ok := r.accounts[addr]; ok {
		return a.Incarnation, nil
	}
	return 0, nil
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
		out[addr] = make(map[state.AccountKey]*state.VersionedRead, len(keys))
		for k, v := range keys {
			cpy := *v
			out[addr][k] = &cpy
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
			BlockGasUsed:         21000,
		},
		Coinbase: s.coinbase,
	}

	return &execResult{TxResult: txResult}
}

// runFinalizeWithIBS runs the IBS-based finalize path.
func (s *testFinalizeScenario) runFinalizeWithIBS(t *testing.T) (state.ReadSet, state.VersionedWrites) {
	t.Helper()
	result := s.buildExecResult()
	result.TxIn = copyReadSet(s.txIn)
	result.TxOut = copyWrites(s.txOut)
	vm := state.NewVersionMap(nil)
	reader := s.makeReader()
	collector := state.NewLightCollector()

	// Strip coinbase/burnt (same as the finalize dispatch does).
	txOut, coinbaseDelta, coinbaseDeltaIncrease, hasCoinbaseDelta := result.TxOut.StripBalanceWrite(result.Coinbase, result.TxIn)
	result.TxOut = txOut
	txOut, burntDelta, burntDeltaIncrease, hasBurntDelta := result.TxOut.StripBalanceWrite(result.ExecutionResult.BurntContractAddress, result.TxIn)
	result.TxOut = txOut
	delete(result.TxIn, result.Coinbase)
	delete(result.TxIn, result.ExecutionResult.BurntContractAddress)

	task := result.Task.(*taskVersion)
	txTask := task.Task.(*exec.TxTask)

	_, reads, writes, err := result.finalizeWithIBS(
		task, txTask, nil, nil, vm, reader, collector,
		coinbaseDelta, coinbaseDeltaIncrease, hasCoinbaseDelta,
		burntDelta, burntDeltaIncrease, hasBurntDelta,
		s.rules, false, "",
	)
	require.NoError(t, err)
	return reads, writes
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
	delete(result.TxIn, result.Coinbase)
	delete(result.TxIn, result.ExecutionResult.BurntContractAddress)

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
		Balance:     *uint256.NewInt(balance),
		Nonce:       nonce,
		Incarnation: 1,
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
	config := &chain.Config{ChainID: big.NewInt(1)}

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
		{Address: sender, Path: state.BalancePath, Val: *newSenderBal, Reason: tracing.BalanceDecreaseGasBuy},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: recipient, Path: state.BalancePath, Val: *newRecipientBal, Reason: tracing.BalanceChangeTransfer},
	}

	// CollectorWrites: LightCollector output from MakeWriteSet.
	// No coinbase (not touched during execution).
	collectorWrites := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *newSenderBal},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: sender, Path: state.IncarnationPath, Val: uint64(1)},
		{Address: sender, Path: state.CodeHashPath, Val: accounts.EmptyCodeHash},
		{Address: recipient, Path: state.BalancePath, Val: *newRecipientBal},
		{Address: recipient, Path: state.NoncePath, Val: uint64(0)},
		{Address: recipient, Path: state.IncarnationPath, Val: uint64(1)},
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
	s.config.LondonBlock = big.NewInt(0)

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

// TestFinalizeIBSPath_SimpleTransfer verifies the IBS finalize path produces
// expected reads and writes for a simple ETH transfer.
func TestFinalizeIBSPath_SimpleTransfer(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()
	reads, writes := s.runFinalizeWithIBS(t)

	assert.NotNil(t, reads, "IBS path should produce reads")
	assert.Greater(t, len(writes), 0, "IBS path should produce writes")

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "should have coinbase balance write")
	coinbaseBalance := coinbaseWrite.Val.(uint256.Int)
	assert.Equal(t, s.feeTipped, coinbaseBalance, "coinbase should receive the tip")
}

// TestFinalizeIBSPath_London verifies the IBS finalize path with burnt fees.
func TestFinalizeIBSPath_London(t *testing.T) {
	t.Parallel()
	s := londonTransferScenario()
	reads, writes := s.runFinalizeWithIBS(t)

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
	config := &chain.Config{ChainID: big.NewInt(1)}

	txIn := state.ReadSet{}
	txIn.Set(state.VersionedRead{Address: sender, Path: state.AddressPath, Val: fMakeAccount(senderBal.Uint64(), 0)})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.BalancePath, Val: *senderBal})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.NoncePath, Val: uint64(0)})
	// Coinbase IS touched during execution (as transfer recipient).
	txIn.Set(state.VersionedRead{Address: coinbase, Path: state.AddressPath, Val: fMakeAccount(coinbaseBal.Uint64(), 0)})
	txIn.Set(state.VersionedRead{Address: coinbase, Path: state.BalancePath, Val: *coinbaseBal})

	// TxOut: coinbase has transfer amount but NOT tip (fees deferred).
	txOut := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *newSenderBal, Reason: tracing.BalanceDecreaseGasBuy},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: coinbase, Path: state.BalancePath, Val: *newCoinbaseBal, Reason: tracing.BalanceChangeTransfer},
	}

	collectorWrites := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *newSenderBal},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: sender, Path: state.IncarnationPath, Val: uint64(1)},
		{Address: sender, Path: state.CodeHashPath, Val: accounts.EmptyCodeHash},
		{Address: coinbase, Path: state.BalancePath, Val: *newCoinbaseBal},
		{Address: coinbase, Path: state.NoncePath, Val: uint64(0)},
		{Address: coinbase, Path: state.IncarnationPath, Val: uint64(1)},
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
	config := &chain.Config{ChainID: big.NewInt(1)}
	tip := uint256.NewInt(21_000)

	txIn := state.ReadSet{}
	txIn.Set(state.VersionedRead{Address: sender, Path: state.AddressPath, Val: fMakeAccount(senderBal.Uint64(), 0)})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.BalancePath, Val: *senderBal})
	txIn.Set(state.VersionedRead{Address: sender, Path: state.NoncePath, Val: uint64(0)})

	txOut := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *senderBal, Reason: tracing.BalanceChangeTransfer},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
	}

	collectorWrites := state.VersionedWrites{
		{Address: sender, Path: state.BalancePath, Val: *senderBal},
		{Address: sender, Path: state.NoncePath, Val: uint64(1)},
		{Address: sender, Path: state.IncarnationPath, Val: uint64(1)},
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

// TestFinalizeTx_MatchesIBS is the key regression test: the direct finalize
// path must produce the same writes and BalancePath reads as the IBS path.
func TestFinalizeTx_MatchesIBS(t *testing.T) {
	t.Parallel()
	scenarios := []*testFinalizeScenario{
		simpleTransferScenario(),
		londonTransferScenario(),
		coinbaseIsRecipientScenario(),
		selfTransferScenario(),
	}

	for _, s := range scenarios {
		t.Run(s.name, func(t *testing.T) {
			ibsReads, ibsWrites := s.runFinalizeWithIBS(t)
			txReads, txWrites := s.runFinalizeTx(t)

			// Compare writes.
			ibsWM := buildWriteMap(ibsWrites)
			txWM := buildWriteMap(txWrites)

			for key, ibsVal := range ibsWM {
				txVal, ok := txWM[key]
				if !ok {
					t.Errorf("finalizeTx missing write: %s = %s", key, ibsVal)
					continue
				}
				if ibsVal != txVal {
					t.Errorf("finalizeTx write mismatch for %s:\n  IBS: %s\n  Tx:  %s", key, ibsVal, txVal)
				}
			}
			for key, txVal := range txWM {
				if _, ok := ibsWM[key]; !ok {
					t.Errorf("finalizeTx has extra write: %s = %s", key, txVal)
				}
			}

			// Compare BalancePath reads (critical for BAL hash).
			ibsBR := extractBalanceReads(ibsReads)
			txBR := extractBalanceReads(txReads)

			for addr, ibsVal := range ibsBR {
				txVal, ok := txBR[addr]
				if !ok {
					t.Errorf("finalizeTx missing BalancePath read for %x", addr)
					continue
				}
				if ibsVal != txVal {
					t.Errorf("finalizeTx BalancePath read mismatch for %x: IBS=%s Tx=%s", addr, ibsVal, txVal)
				}
			}
			for addr := range txBR {
				if _, ok := ibsBR[addr]; !ok {
					t.Errorf("finalizeTx has extra BalancePath read for %x", addr)
				}
			}
		})
	}
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
