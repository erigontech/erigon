package stagedsync

import (
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// senderIsCoinbaseKeyHolder pairs a deterministic test key with its derived
// address in both raw common.Address (needed by SignTx + Tx.To) and interned
// accounts.Address (needed by the versioned write set) forms.
type senderIsCoinbaseKeyHolder struct {
	key        *ecdsa.PrivateKey
	rawAddress common.Address
	address    accounts.Address
}

// senderIsCoinbaseKey is a deterministic test key whose derived address is
// used as BOTH the sender and the coinbase in the sender==coinbase regression
// scenarios. The same hex constant appears in stage_senders_test.go.
var senderIsCoinbaseKey = func() *senderIsCoinbaseKeyHolder {
	k, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	if err != nil {
		panic(err)
	}
	rawAddr := crypto.PubkeyToAddress(k.PublicKey)
	return &senderIsCoinbaseKeyHolder{
		key:        k,
		rawAddress: rawAddr,
		address:    accounts.InternAddress(rawAddr),
	}
}()

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
	// txs is optional: when set, buildExecResult attaches it to the TxTask
	// so finalizeTxSimple's TxMessage().From() call returns the signer's
	// derived sender. Required for senderIsCoinbase test coverage.
	txs []types.Transaction
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
		Txs:     s.txs, // nil when scenario doesn't need TxMessage().From()
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

// signSelfSendTx builds a legacy transaction where sender == recipient ==
// the senderIsCoinbaseKey's derived address (and BOTH will be the
// scenario's coinbase). The signer is derived from `config` at block 1, so
// `config` must have all the relevant pre-fork blocks set to 0 (e.g.
// TestChainBerlinConfig). Returns the signed transaction.
//
// Using a real signed tx (rather than fabricating a *types.Message
// directly) is necessary because finalizeTxSimple's senderIsCoinbase
// detection calls txTask.TxMessage(), which lazy-derives the message from
// the signed tx via the chain's signer — there is no public setter for the
// unexported `message` field on *exec.TxTask.
func signSelfSendTx(t *testing.T, nonce uint64, value uint64, gasPrice uint64, gasLimit uint64, config *chain.Config, blockTime uint64) types.Transaction {
	t.Helper()
	signer := types.MakeSigner(config, 1, blockTime)
	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    nonce,
			To:       &senderIsCoinbaseKey.rawAddress,
			Value:    *uint256.NewInt(value),
			GasLimit: gasLimit,
		},
		GasPrice: *uint256.NewInt(gasPrice),
	}
	signed, err := types.SignTx(tx, *signer, senderIsCoinbaseKey.key)
	require.NoError(t, err, "SignTx must succeed for the deterministic test key")
	return signed
}

// TestSignSelfSendTx_SmokeTestForScaffolding verifies that the
// signSelfSendTx helper produces a transaction whose recovered sender
// matches senderIsCoinbaseKey.address. This is the critical contract the
// senderIsCoinbase test family depends on — without it, those tests would
// silently exercise the wrong code path inside finalizeTxSimple.
func TestSignSelfSendTx_SmokeTestForScaffolding(t *testing.T) {
	t.Parallel()

	config := chain.TestChainBerlinConfig
	signedTx := signSelfSendTx(t, 0 /* nonce */, 0 /* value */, 1 /* gasPrice */, 21000 /* gasLimit */, config, 0 /* blockTime */)

	// Construct a minimal TxTask carrying this signed tx, then call
	// TxMessage() — same path finalizeTxSimple uses.
	header := &types.Header{Number: *uint256.NewInt(1)}
	txTask := &exec.TxTask{
		Header:  header,
		TxNum:   1,
		TxIndex: 0,
		Config:  config,
		Txs:     []types.Transaction{signedTx},
	}

	msg, err := txTask.TxMessage()
	require.NoError(t, err, "TxMessage must succeed on a properly signed tx")
	require.NotNil(t, msg, "TxMessage must return a non-nil message")

	// msg.From() returns common.Address — intern it to match how
	// finalizeTxSimple's senderIsCoinbase check works.
	recovered := accounts.InternAddress(msg.From().Value())
	require.Equal(t, senderIsCoinbaseKey.address, recovered,
		"recovered sender must equal the key's derived address; otherwise the senderIsCoinbase tests are unsound")
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

// senderIsCoinbaseScenario builds a scenario where sender == coinbase (via
// a real signed tx so finalizeTxSimple's TxMessage().From() == result.Coinbase
// check returns true). The TxOut and CollectorWrites shapes are minimal
// baselines — each test customizes them to exercise its specific case.
//
// preBlockCoinbaseBal is the pre-block coinbase balance written into the
// reader's accts map. tip is the FeeTipped value the worker-skipped tip
// credit must be re-added on top of by finalizeTxSimple.
//
// london controls whether the chain config has LondonBlock=0 (and the rules
// have IsLondon=true) and whether a burnt contract address + FeeBurnt are
// populated on the result.
func senderIsCoinbaseScenario(t *testing.T, value uint64, preBlockCoinbaseBal uint64, tip uint64, london bool) *testFinalizeScenario {
	t.Helper()

	coinbase := senderIsCoinbaseKey.address

	var config *chain.Config
	var rules *chain.Rules
	var header *types.Header
	if london {
		// London-enabled config + rules — fresh construction (cannot
		// dereference-copy chain.Config; it embeds sync.Once via noCopy).
		config = &chain.Config{
			ChainID:               uint256.NewInt(1337),
			Rules:                 chain.EtHashRules,
			HomesteadBlock:        common.NewUint64(0),
			TangerineWhistleBlock: common.NewUint64(0),
			SpuriousDragonBlock:   common.NewUint64(0),
			ByzantiumBlock:        common.NewUint64(0),
			ConstantinopleBlock:   common.NewUint64(0),
			PetersburgBlock:       common.NewUint64(0),
			IstanbulBlock:         common.NewUint64(0),
			MuirGlacierBlock:      common.NewUint64(0),
			BerlinBlock:           common.NewUint64(0),
			LondonBlock:           common.NewUint64(0),
			Ethash:                new(chain.EthashConfig),
		}
		rules = &chain.Rules{IsSpuriousDragon: true, IsLondon: true}
		header = &types.Header{
			Number:   *uint256.NewInt(1),
			GasLimit: 30_000_000,
			GasUsed:  21000,
			BaseFee:  uint256.NewInt(1), // required for IsLondon signer
		}
	} else {
		config = chain.TestChainBerlinConfig
		rules = &chain.Rules{IsSpuriousDragon: true}
		header = &types.Header{
			Number:   *uint256.NewInt(1),
			GasLimit: 30_000_000,
			GasUsed:  21000,
		}
	}

	signed := signSelfSendTx(t, 0 /* nonce */, value, 1 /* gasPrice */, 21000 /* gasLimit */, config, 0 /* blockTime */)

	// Baseline TxOut: sender (=coinbase) nonce bumped to 1. Tests customise
	// the coinbase BalancePath entry to exercise specific worker-output
	// shapes.
	txOut := state.VersionedWrites{
		{Address: coinbase, Path: state.NoncePath, Val: uint64(1)},
	}

	// Baseline CollectorWrites: matches TxOut by default. Tests customise
	// to exercise the senderIsCoinbase discriminator.
	collectorWrites := state.VersionedWrites{
		{Address: coinbase, Path: state.NoncePath, Val: uint64(1)},
		{Address: coinbase, Path: state.IncarnationPath, Val: uint64(1)},
		{Address: coinbase, Path: state.CodeHashPath, Val: accounts.EmptyCodeHash},
	}

	// Baseline TxIn: sender (=coinbase) balance + nonce reads.
	txIn := state.ReadSet{}
	txIn.Set(state.VersionedRead{Address: coinbase, Path: state.AddressPath, Val: fMakeAccount(preBlockCoinbaseBal, 0)})
	txIn.Set(state.VersionedRead{Address: coinbase, Path: state.BalancePath, Val: *uint256.NewInt(preBlockCoinbaseBal)})
	txIn.Set(state.VersionedRead{Address: coinbase, Path: state.NoncePath, Val: uint64(0)})

	scenario := &testFinalizeScenario{
		name: "sender_is_coinbase",
		accts: map[accounts.Address]*accounts.Account{
			coinbase: fMakeAccount(preBlockCoinbaseBal, 0),
		},
		txIn:            txIn,
		txOut:           txOut,
		collectorWrites: collectorWrites,
		feeTipped:       *uint256.NewInt(tip),
		coinbase:        coinbase,
		burntAddr:       accounts.NilAddress,
		rules:           rules,
		config:          config,
		header:          header,
		txs:             []types.Transaction{signed},
	}

	if london {
		// Post-London: include a burnt contract address + a small FeeBurnt.
		burntAddr := fAddr("burntcontract")
		scenario.burntAddr = burntAddr
		scenario.feeBurnt = *uint256.NewInt(1000)
		scenario.accts[burntAddr] = fMakeAccount(500_000, 0)
	}

	return scenario
}

func findWrite(writes state.VersionedWrites, addr accounts.Address, path state.AccountPath) *state.VersionedWrite {
	for _, w := range writes {
		if w.Address == addr && w.Path == path {
			return w
		}
	}
	return nil
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

// TestFinalizeTxSimple_SenderIsCoinbase_TxOutValueWins is the regression
// pin for bug #1 of #21017. The block-218957 manifestation:
//
//   - Sender == coinbase (a Frontier-style miner self-send)
//   - Worker runs with shouldDelayFeeCalc=true so noFeeBurnAndTip=true.
//     buyGas still debits the sender; the tip credit to coinbase is skipped
//     (it's what finalize must add back).
//   - The worker's IBS therefore has coinbase Balance debited by the gas
//     amount. TxOut (raw IBS.VersionedWrites output) carries this debited
//     value.
//   - For Frontier miner self-sends, CollectorWrites (IBS net-change via
//     LightCollector) suppresses the coinbase entry under specific
//     conditions documented at exec3_parallel.go:1641-1673 — historically
//     the load-bearing case the bug originally hit.
//
// Pre-fix: finalizeTxSimple scanned CollectorWrites only. The suppression
// meant no override → newCoinbaseBalance stayed at the versionMap base
// (pre-this-tx value) → adding FeeTipped on top over-credited the coinbase
// by exactly one tip.
//
// Post-fix: when senderIsCoinbase, finalizeTxSimple scans TxOut instead,
// finds the worker's debited value, overrides newCoinbaseBalance, then
// adds FeeTipped. The net result equals the canonical post-tx state
// (debit and tip cancel for a miner self-send).
//
// This test pins the discriminator: when sender==coinbase AND TxOut has
// a coinbase BalancePath entry that disagrees with the versionMap base,
// finalize MUST use the TxOut value as the base for the tip credit.
func TestFinalizeTxSimple_SenderIsCoinbase_TxOutValueWins(t *testing.T) {
	t.Parallel()

	const (
		preBlockBal   = uint64(1_000_000)
		gasCost       = uint64(21_000) // gasLimit * gasPrice for the test tx
		postDebitBal  = preBlockBal - gasCost
		tip           = gasCost // Frontier: effectiveTip = gasPrice; tip = gasUsed * gasPrice = gasCost
		expectedFinal = postDebitBal + tip
		_             = expectedFinal // documents the invariant: cancels back to preBlockBal
	)

	s := senderIsCoinbaseScenario(t, 0 /* value=0: pure miner self-send */, preBlockBal, tip, false /* pre-London */)

	// TxOut: worker debited coinbase by gas — emit the post-debit value.
	// CollectorWrites: SUPPRESS the coinbase BalancePath entry (mimicking
	// the bug-trigger condition). The two disagree; the senderIsCoinbase
	// discriminator must pick TxOut's value.
	s.txOut = append(s.txOut, &state.VersionedWrite{
		Address: s.coinbase,
		Path:    state.BalancePath,
		Val:     *uint256.NewInt(postDebitBal),
	})
	// (s.collectorWrites left as the baseline — no coinbase BalancePath entry)

	writes := s.runFinalizeTxSimple(t, nil /* no prior tx in this block */)

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "finalize must produce a coinbase BalancePath write")

	got := coinbaseWrite.Val.(uint256.Int)
	want := uint256.NewInt(expectedFinal)
	assert.Equal(t, *want, got,
		"senderIsCoinbase: finalize must use TxOut value (%d) + tip (%d) = %d, NOT versionMap base (%d) + tip (%d) = %d (the bug)",
		postDebitBal, tip, expectedFinal, preBlockBal, tip, preBlockBal+tip)
}

// TestFinalizeTxSimple_SenderNotCoinbase_CollectorWritesValueWins is the
// negative control for TxOutValueWins. Same TxOut shape (coinbase
// BalancePath present), but sender != coinbase, so the senderIsCoinbase
// discriminator falls into the else branch (scan CollectorWrites instead).
//
// CollectorWrites has NO coinbase BalancePath entry, so no override
// happens — finalizeTxSimple uses the versionMap base + tip. Proves that
// the senderIsCoinbase branch in TxOutValueWins is the load-bearing
// reason that test passes (i.e. flipping just the sender flips the
// outcome), rather than some unrelated code path masking the bug.
func TestFinalizeTxSimple_SenderNotCoinbase_CollectorWritesValueWins(t *testing.T) {
	t.Parallel()

	// Use simpleTransferScenario: sender (fAddr("sender")) != coinbase
	// (fAddr("coinbase")). No s.txs set → txTask.TxMessage() returns nil
	// → senderIsCoinbase stays false → scans CollectorWrites only.
	s := simpleTransferScenario()

	// Force the same TxOut-only-coinbase shape as the positive test:
	// coinbase BalancePath = some-non-zero value in TxOut, NO coinbase
	// BalancePath in CollectorWrites. Pre-block coinbase balance is 0
	// (from simpleTransferScenario.accts).
	const (
		txOutCoinbaseBal = uint64(979_000) // arbitrary "post-debit" value
		preBlockCoinbase = uint64(0)       // simpleTransferScenario default
		tip              = uint64(21_000)  // from scenario.feeTipped
	)
	s.txOut = append(s.txOut, &state.VersionedWrite{
		Address: s.coinbase,
		Path:    state.BalancePath,
		Val:     *uint256.NewInt(txOutCoinbaseBal),
	})
	// CollectorWrites unchanged — no coinbase BalancePath entry in baseline.

	writes := s.runFinalizeTxSimple(t, nil)

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "finalize should produce a coinbase BalancePath write")

	got := coinbaseWrite.Val.(uint256.Int)
	expected := uint256.NewInt(preBlockCoinbase + tip)
	assert.Equal(t, *expected, got,
		"sender != coinbase: finalize must use versionMap base (%d) + tip (%d) = %d, NOT TxOut value (%d) + tip = %d",
		preBlockCoinbase, tip, preBlockCoinbase+tip, txOutCoinbaseBal, txOutCoinbaseBal+tip)
}

// TestFinalizeTxSimple_SenderIsCoinbase_TxOutWinsOverCollectorWrites
// strengthens TxOutValueWins by adding a coinbase BalancePath entry to
// CollectorWrites at a DIFFERENT value than TxOut. The discriminator must
// firmly pick TxOut when senderIsCoinbase=true — even when CollectorWrites
// is non-empty and would have given a different answer.
//
// Maps to cleanup memory case #2: sender == coinbase, value > 0, Frontier.
// In real EVM exec, a sender==coinbase value-bearing self-send produces a
// different CollectorWrites shape than a value=0 self-send because the
// IBS journal records the self-transfer's SubBalance/AddBalance pair (net
// zero on the address but tracked by LightCollector's per-call deltas). The
// test pins that the discriminator's choice does not depend on whether
// CollectorWrites has an entry.
func TestFinalizeTxSimple_SenderIsCoinbase_TxOutWinsOverCollectorWrites(t *testing.T) {
	t.Parallel()

	const (
		preBlockBal   = uint64(1_000_000)
		postDebitBal  = uint64(979_000)   // TxOut: gas-debited value
		collectorBal  = uint64(1_500_000) // CollectorWrites: different, wrong value
		tip           = uint64(21_000)
		expectedFinal = postDebitBal + tip // 1,000,000 — must use TxOut, not CollectorWrites
	)

	s := senderIsCoinbaseScenario(t, 100 /* value > 0 self-send */, preBlockBal, tip, false)

	s.txOut = append(s.txOut, &state.VersionedWrite{
		Address: s.coinbase,
		Path:    state.BalancePath,
		Val:     *uint256.NewInt(postDebitBal),
	})
	// Add a contradictory CollectorWrites entry to prove the discriminator
	// firmly picks TxOut. If finalize ever falls through to scanning
	// CollectorWrites under senderIsCoinbase=true, the assertion below
	// would fail with the collectorBal+tip value.
	s.collectorWrites = append(s.collectorWrites, &state.VersionedWrite{
		Address: s.coinbase,
		Path:    state.BalancePath,
		Val:     *uint256.NewInt(collectorBal),
	})

	writes := s.runFinalizeTxSimple(t, nil)

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "finalize must produce coinbase BalancePath write")

	got := coinbaseWrite.Val.(uint256.Int)
	want := uint256.NewInt(expectedFinal)
	assert.Equal(t, *want, got,
		"senderIsCoinbase=true: TxOut value (%d) + tip (%d) = %d MUST WIN over CollectorWrites value (%d) + tip = %d",
		postDebitBal, tip, expectedFinal, collectorBal, collectorBal+tip)
}

// TestFinalizeTxSimple_SenderIsCoinbase_PostLondon verifies the
// senderIsCoinbase discriminator still picks TxOut for the coinbase under
// post-London rules (IsLondon=true, separate burnt contract address). The
// burnt path runs in parallel for a non-sender burnt contract — its
// FeeBurnt is added to its versionMap base via the regular (non-sender)
// CollectorWrites scan.
//
// Maps to cleanup memory case #3: sender == coinbase, value > 0, post-London.
func TestFinalizeTxSimple_SenderIsCoinbase_PostLondon(t *testing.T) {
	t.Parallel()

	const (
		preBlockBal      = uint64(1_000_000)
		postDebitBal     = uint64(979_000)
		tip              = uint64(11_000)
		burn             = uint64(10_000)
		burntPreBlockBal = uint64(500_000)
		expectedCoinbase = postDebitBal + tip      // 990,000
		expectedBurntBal = burntPreBlockBal + burn // 510,000
	)

	s := senderIsCoinbaseScenario(t, 0, preBlockBal, tip, true /* london */)
	s.feeBurnt = *uint256.NewInt(burn) // override scenario default

	s.txOut = append(s.txOut, &state.VersionedWrite{
		Address: s.coinbase,
		Path:    state.BalancePath,
		Val:     *uint256.NewInt(postDebitBal),
	})
	// CollectorWrites: no coinbase, no burnt — finalize reads burnt base
	// from versionMap (= burntPreBlockBal from scenario.accts) and adds burn.

	writes := s.runFinalizeTxSimple(t, nil)

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "coinbase BalancePath write must exist")
	gotCoinbase := coinbaseWrite.Val.(uint256.Int)
	assert.Equal(t, *uint256.NewInt(expectedCoinbase), gotCoinbase,
		"coinbase: TxOut (%d) + tip (%d) = %d expected", postDebitBal, tip, expectedCoinbase)

	burntWrite := findWrite(writes, s.burntAddr, state.BalancePath)
	require.NotNil(t, burntWrite, "burnt BalancePath write must exist under London")
	gotBurnt := burntWrite.Val.(uint256.Int)
	assert.Equal(t, *uint256.NewInt(expectedBurntBal), gotBurnt,
		"burnt: versionMap base (%d) + burn (%d) = %d expected", burntPreBlockBal, burn, expectedBurntBal)
}

// TestFinalizeTxSimple_SenderIsCoinbase_AccumulatedAcrossTxs runs three
// successive sender==coinbase txs through finalize, verifying that the tip
// credit accumulates correctly across the txs even when the discriminator
// uses TxOut on each call. Mirrors the existing
// TestFinalizeTxSimple_AccumulatedFees but for sender==coinbase.
//
// Maps to cleanup memory case #6: multiple sender == coinbase txs in same
// block. Pins that the per-tx TxOut-override doesn't break across-tx
// accumulation — each tx's worker debit + finalize tip credit cancel,
// so the coinbase balance stays at preBlockBal across all N txs.
func TestFinalizeTxSimple_SenderIsCoinbase_AccumulatedAcrossTxs(t *testing.T) {
	t.Parallel()

	const (
		preBlockBal  = uint64(1_000_000)
		gasCost      = uint64(21_000)
		postDebitBal = preBlockBal - gasCost
		tip          = gasCost
		numTxs       = 3
	)

	// One scenario sets up the stateReader + chain config. Each tx uses
	// a freshly built execResult to avoid state leakage between iterations.
	baseScenario := senderIsCoinbaseScenario(t, 0, preBlockBal, tip, false)
	vm := state.NewVersionMap(nil)
	reader := baseScenario.makeReader()

	for txIdx := 0; txIdx < numTxs; txIdx++ {
		s := senderIsCoinbaseScenario(t, 0, preBlockBal, tip, false)
		// Each tx: TxOut has coinbase at postDebitBal. Since each tx
		// nets coinbase back to preBlockBal after finalize, the next
		// tx still sees preBlockBal as its base.
		s.txOut = append(s.txOut, &state.VersionedWrite{
			Address: s.coinbase,
			Path:    state.BalancePath,
			Val:     *uint256.NewInt(postDebitBal),
		})

		// Stamp each TxOut entry with this iteration's Version before
		// flushing so the writes land at (txIdx, 0). Without this, the
		// scenario baseline's unstamped writes default to (0, 0) and
		// collide with the prior iteration's finalize tip-credit writes
		// at (prevTxIdx, 1) — versionMap.writeLocked panics when the new
		// incarnation is lower than the existing cell's.
		iterVersion := state.Version{BlockNum: 1, TxNum: uint64(txIdx + 1), TxIndex: txIdx, Incarnation: 0}
		for _, w := range s.txOut {
			w.Version = iterVersion
		}

		result := s.buildExecResult()
		result.TxIn = copyReadSet(s.txIn)
		result.TxOut = copyWrites(s.txOut)
		result.CollectorWrites = copyWrites(s.collectorWrites)

		// Set this tx's version explicitly so versionMap reads land on
		// the right tx-index for the floor-read semantics.
		task := result.Task.(*taskVersion)
		task.version = iterVersion

		vm.FlushVersionedWrites(result.TxOut, true, "")

		txTask := task.Task.(*exec.TxTask)
		_, _, writes, err := result.finalizeTxSimple(
			task, txTask, nil, nil, vm, reader,
			s.rules, false, false, "",
		)
		require.NoError(t, err, "tx %d: finalizeTxSimple", txIdx)

		// Flush finalize writes so the next tx sees them via versionMap.
		vm.FlushVersionedWrites(writes, true, "")

		coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
		require.NotNil(t, coinbaseWrite, "tx %d: coinbase BalancePath write must exist (workerWroteCoinbase gate fires when newBal==oldBal)", txIdx)
		got := coinbaseWrite.Val.(uint256.Int)
		want := uint256.NewInt(postDebitBal + tip) // = preBlockBal: each tx cancels back
		assert.Equal(t, *want, got, "tx %d: coinbase must net to preBlockBal", txIdx)
	}
}

// TestFinalizeTxSimple_SenderIsCoinbase_ReExecutedIncarnation pins that
// when the worker re-executes at incarnation > 0 and emits a different
// TxOut value than the abandoned incarnation 0, finalize uses the
// RE-EXECUTED value (from the latest TxOut), not the abandoned one.
//
// Maps to cleanup memory case #7: sender == coinbase with worker
// re-execution. Tests that the TxOut scan in finalize is on the CURRENT
// execResult's TxOut (not stashed from a prior incarnation).
func TestFinalizeTxSimple_SenderIsCoinbase_ReExecutedIncarnation(t *testing.T) {
	t.Parallel()

	const (
		preBlockBal       = uint64(1_000_000)
		abandonedPostBal  = uint64(950_000) // what incarnation 0's worker would have produced (e.g. wrong gas)
		reExecutedPostBal = uint64(979_000) // what incarnation 1's worker produced (correct)
		tip               = uint64(21_000)
		expectedFinal     = reExecutedPostBal + tip // = 1,000,000
	)

	s := senderIsCoinbaseScenario(t, 0, preBlockBal, tip, false)

	// Simulate the abandoned incarnation 0 write already in versionMap.
	// finalize's vsReader uses floor(txIndex-1) so it WON'T read this
	// value (TxIndex=0), but recording it documents the scenario.
	// What matters for the test is that the CURRENT result.TxOut is what
	// finalize scans.
	s.txOut = append(s.txOut, &state.VersionedWrite{
		Address: s.coinbase,
		Path:    state.BalancePath,
		Val:     *uint256.NewInt(reExecutedPostBal), // the re-executed (correct) value
	})

	// Set incarnation > 0 on the task to reflect re-execution.
	result := s.buildExecResult()
	result.TxIn = copyReadSet(s.txIn)
	result.TxOut = copyWrites(s.txOut)
	result.CollectorWrites = copyWrites(s.collectorWrites)

	task := result.Task.(*taskVersion)
	task.version.Incarnation = 1 // re-execution

	vm := state.NewVersionMap(nil)
	reader := s.makeReader()

	// Pre-populate versionMap with the abandoned incarnation 0 value at
	// (txIndex=0, incarnation=0). The re-execution at incarnation=1 should
	// produce a write that masks this; finalize must NOT use this stale
	// abandoned value.
	vm.Write(s.coinbase, state.BalancePath, accounts.NilKey,
		state.Version{TxIndex: 0, Incarnation: 0},
		*uint256.NewInt(abandonedPostBal), true)

	// Now flush the re-executed TxOut at incarnation 1.
	vm.FlushVersionedWrites(result.TxOut, true, "")

	txTask := task.Task.(*exec.TxTask)
	_, _, writes, err := result.finalizeTxSimple(
		task, txTask, nil, nil, vm, reader,
		s.rules, false, false, "",
	)
	require.NoError(t, err)

	coinbaseWrite := findWrite(writes, s.coinbase, state.BalancePath)
	require.NotNil(t, coinbaseWrite, "coinbase BalancePath write must exist")
	got := coinbaseWrite.Val.(uint256.Int)
	want := uint256.NewInt(expectedFinal)
	assert.Equal(t, *want, got,
		"re-execution: finalize must use re-executed TxOut value (%d) + tip (%d) = %d, NOT abandoned incarnation-0 value (%d) + tip = %d",
		reExecutedPostBal, tip, expectedFinal, abandonedPostBal, abandonedPostBal+tip)
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
		{Address: addr, Path: state.IncarnationPath, Val: uint64(1),
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
		{Address: addr, Path: state.CodeHashPath, Val: codeHash,
			Version: state.Version{TxIndex: 0, Incarnation: 0}},
	}
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, nil, nil, true)

	// Should have CreateContractPath + all 4 account fields.
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
