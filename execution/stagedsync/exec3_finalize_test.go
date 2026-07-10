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
	"github.com/erigontech/erigon/execution/protocol/params"
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
	txOut           *state.WriteSet
	collectorWrites *state.WriteSet
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

// copyReadSet makes a copy of a ReadSet for test isolation.
func copyReadSet(rs state.ReadSet) state.ReadSet {
	return rs.Merge(state.ReadSet{})
}

// copyWrites makes a deep copy of a WriteSet so a test scenario's baseline
// survives in-place mutation (StripBalanceWrite etc.) across repeated runs.
func copyWrites(ws *state.WriteSet) *state.WriteSet {
	if ws == nil {
		return nil
	}
	out := &state.WriteSet{}
	for h := range ws.AllHeaders() {
		if h.Path == state.AddressPath {
			if w, ok := ws.GetAddress(h.Address); ok {
				cp := *w
				out.SetAddress(h.Address, &cp)
			}
		}
		if h.Path == state.CreateContractPath {
			if w, ok := ws.GetCreateContract(h.Address); ok {
				cp := *w
				out.SetCreateContract(h.Address, &cp)
			}
		}
		if h.Path == state.CodeSizePath {
			if w, ok := ws.GetCodeSize(h.Address); ok {
				cp := *w
				out.SetCodeSize(h.Address, &cp)
			}
		}
	}
	for addr, w := range ws.Balances() {
		cp := *w
		out.SetBalance(addr, &cp)
	}
	for addr, w := range ws.Nonces() {
		cp := *w
		out.SetNonce(addr, &cp)
	}
	for addr, w := range ws.Incarnations() {
		cp := *w
		out.SetIncarnation(addr, &cp)
	}
	for addr, w := range ws.SelfDestructs() {
		cp := *w
		out.SetSelfDestruct(addr, &cp)
	}
	for addr, w := range ws.Codes() {
		cp := *w
		out.SetCode(addr, &cp)
	}
	for addr, w := range ws.CodeHashes() {
		cp := *w
		out.SetCodeHash(addr, &cp)
	}
	for addr, inner := range ws.Storages() {
		for key, w := range inner {
			cp := *w
			out.SetStorage(addr, key, &cp)
		}
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
	txIn.SetAddress(sender, state.VersionedRead[state.AccountView]{Val: state.NewAccountView(fMakeAccount(senderBal.Uint64(), 0))})
	txIn.SetBalance(sender, state.VersionedRead[uint256.Int]{Val: *senderBal})
	txIn.SetNonce(sender, state.VersionedRead[uint64]{Val: uint64(0)})
	txIn.SetAddress(recipient, state.VersionedRead[state.AccountView]{Val: state.NewAccountView(fMakeAccount(recipientBal.Uint64(), 0))})
	txIn.SetBalance(recipient, state.VersionedRead[uint256.Int]{Val: *recipientBal})

	// TxOut: writes from execution. No coinbase write (fees deferred).
	txOut := &state.WriteSet{}
	txOut.SetBalance(sender, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: sender, Path: state.BalancePath, Reason: tracing.BalanceDecreaseGasBuy}, Val: *newSenderBal})
	txOut.SetNonce(sender, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: sender, Path: state.NoncePath}, Val: uint64(1)})
	txOut.SetBalance(recipient, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: recipient, Path: state.BalancePath, Reason: tracing.BalanceChangeTransfer}, Val: *newRecipientBal})

	// CollectorWrites: LightCollector output from MakeWriteSet.
	// No coinbase (not touched during execution).
	collectorWrites := &state.WriteSet{}
	collectorWrites.SetBalance(sender, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: sender, Path: state.BalancePath}, Val: *newSenderBal})
	collectorWrites.SetNonce(sender, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: sender, Path: state.NoncePath}, Val: uint64(1)})
	collectorWrites.SetIncarnation(sender, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: sender, Path: state.IncarnationPath}, Val: uint64(1)})
	collectorWrites.SetCodeHash(sender, &state.VersionedWrite[accounts.CodeHash]{WriteHeader: state.WriteHeader{Address: sender, Path: state.CodeHashPath}, Val: accounts.EmptyCodeHash})
	collectorWrites.SetBalance(recipient, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: recipient, Path: state.BalancePath}, Val: *newRecipientBal})
	collectorWrites.SetNonce(recipient, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: recipient, Path: state.NoncePath}, Val: uint64(0)})
	collectorWrites.SetIncarnation(recipient, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: recipient, Path: state.IncarnationPath}, Val: uint64(1)})
	collectorWrites.SetCodeHash(recipient, &state.VersionedWrite[accounts.CodeHash]{WriteHeader: state.WriteHeader{Address: recipient, Path: state.CodeHashPath}, Val: accounts.EmptyCodeHash})

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
	txOut := &state.WriteSet{}
	txOut.SetNonce(coinbase, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: coinbase, Path: state.NoncePath}, Val: uint64(1)})

	// Baseline CollectorWrites: matches TxOut by default. Tests customise
	// to exercise the senderIsCoinbase discriminator.
	collectorWrites := &state.WriteSet{}
	collectorWrites.SetNonce(coinbase, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: coinbase, Path: state.NoncePath}, Val: uint64(1)})
	collectorWrites.SetIncarnation(coinbase, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: coinbase, Path: state.IncarnationPath}, Val: uint64(1)})
	collectorWrites.SetCodeHash(coinbase, &state.VersionedWrite[accounts.CodeHash]{WriteHeader: state.WriteHeader{Address: coinbase, Path: state.CodeHashPath}, Val: accounts.EmptyCodeHash})

	// Baseline TxIn: sender (=coinbase) balance + nonce reads.
	txIn := state.ReadSet{}
	txIn.SetAddress(coinbase, state.VersionedRead[state.AccountView]{Val: state.NewAccountView(fMakeAccount(preBlockCoinbaseBal, 0))})
	txIn.SetBalance(coinbase, state.VersionedRead[uint256.Int]{Val: *uint256.NewInt(preBlockCoinbaseBal)})
	txIn.SetNonce(coinbase, state.VersionedRead[uint64]{Val: uint64(0)})

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

func findBalance(writes *state.WriteSet, addr accounts.Address) *state.VersionedWrite[uint256.Int] {
	if w, ok := writes.GetBalance(addr); ok {
		return w
	}
	return nil
}

func findAddress(writes *state.WriteSet, addr accounts.Address) *state.VersionedWrite[*accounts.Account] {
	if w, ok := writes.GetAddress(addr); ok {
		return w
	}
	return nil
}

// --- finalizeTxSimple tests ---
// These test the split fee logic: worker debits sender only,
// finalize credits coinbase/burnt. No StripBalanceWrite or delta computation.

// runFinalizeTx runs the apply-loop tip credit (calcFees) and returns the
// resulting writes. priorCoinbaseBalance is the pre-block baseline returned
// by stateReader.ReadAccountData(coinbase). calcFees reads at txIndex
// (floor txIndex-1) — strictly prior tx — so for the first tx of a block
// the baseline comes from stateReader, not the versionMap.
func (s *testFinalizeScenario) runFinalizeTx(t *testing.T, priorCoinbaseBalance *uint256.Int) *state.WriteSet {
	t.Helper()
	result := s.buildExecResult()
	result.TxIn = copyReadSet(s.txIn)
	result.TxOut = copyWrites(s.txOut)
	if s.collectorWrites != nil {
		result.CollectorWrites = copyWrites(s.collectorWrites)
	}

	vm := state.NewVersionMap(nil)
	reader := s.makeReader()

	if priorCoinbaseBalance != nil {
		acc, ok := reader.accounts[s.coinbase]
		if !ok {
			acc = &accounts.Account{}
			reader.accounts[s.coinbase] = acc
		}
		acc.Balance = *priorCoinbaseBalance
	}

	vm.FlushVersionedWrites(result.TxOut, true, "")

	task := result.Task.(*taskVersion)

	writes, err := result.calcFees(task, vm, reader, s.rules)
	require.NoError(t, err)
	return writes
}

// TestFinalizeTxSimple_BasicFeeCredit verifies that the apply-loop tip
// credit adds FeeTipped to the coinbase balance. calcFees reads at
// vm.Read(..., txIndex) = floor(txIndex-1), i.e. strictly prior tx; for
// the test's TxIndex=0 case that falls through to stateReader (where
// runFinalizeTx places the prior balance), and the worker's
// current-tx execution effects come in via the TxOut scan.
func TestFinalizeTxSimple_BasicFeeCredit(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()

	// Coinbase starts with 1 ETH (from prior TX's finalize).
	priorBalance := uint256.NewInt(1_000_000_000_000_000_000)
	writes := s.runFinalizeTx(t, priorBalance)

	coinbaseWrite := findBalance(writes, s.coinbase)
	require.NotNil(t, coinbaseWrite, "finalize should produce coinbase BalancePath write")

	coinbaseBalance := coinbaseWrite.Val
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
	s.txOut.SetBalance(s.coinbase, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: s.coinbase, Path: state.BalancePath},
		Val:         *uint256.NewInt(postDebitBal),
	})
	// (s.collectorWrites left as the baseline — no coinbase BalancePath entry)

	writes := s.runFinalizeTx(t, nil /* no prior tx in this block */)

	coinbaseWrite := findBalance(writes, s.coinbase)
	require.NotNil(t, coinbaseWrite, "finalize must produce a coinbase BalancePath write")

	got := coinbaseWrite.Val
	want := uint256.NewInt(expectedFinal)
	assert.Equal(t, *want, got,
		"senderIsCoinbase: finalize must use TxOut value (%d) + tip (%d) = %d, NOT versionMap base (%d) + tip (%d) = %d (the bug)",
		postDebitBal, tip, expectedFinal, preBlockBal, tip, preBlockBal+tip)
}

// (Removed: TestFinalizeTxSimple_SenderNotCoinbase_CollectorWritesValueWins
// was a negative control for the prior code's senderIsCoinbase discriminator
// — it asserted that flipping sender to non-coinbase made finalize ignore
// TxOut and use versionMap base + tip. The new apply-loop calcFees
// step uses vm.Read(..., txIndex+1) for the base, which reads whatever is
// most recently in versionMap at or before this tx — including the TxOut
// entry flushed by the test setup. The discriminator code path no longer
// exists, and the synthetic scenario the test constructed doesn't map to
// any real execution behavior. The SenderIsCoinbase positive tests still
// pin the correctness of the new read semantics.)

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

	s.txOut.SetBalance(s.coinbase, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: s.coinbase, Path: state.BalancePath},
		Val:         *uint256.NewInt(postDebitBal),
	})
	// Add a contradictory CollectorWrites entry to prove the discriminator
	// firmly picks TxOut. If finalize ever falls through to scanning
	// CollectorWrites under senderIsCoinbase=true, the assertion below
	// would fail with the collectorBal+tip value.
	s.collectorWrites.SetBalance(s.coinbase, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: s.coinbase, Path: state.BalancePath},
		Val:         *uint256.NewInt(collectorBal),
	})

	writes := s.runFinalizeTx(t, nil)

	coinbaseWrite := findBalance(writes, s.coinbase)
	require.NotNil(t, coinbaseWrite, "finalize must produce coinbase BalancePath write")

	got := coinbaseWrite.Val
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

	s.txOut.SetBalance(s.coinbase, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: s.coinbase, Path: state.BalancePath},
		Val:         *uint256.NewInt(postDebitBal),
	})
	// CollectorWrites: no coinbase, no burnt — finalize reads burnt base
	// from versionMap (= burntPreBlockBal from scenario.accts) and adds burn.

	writes := s.runFinalizeTx(t, nil)

	coinbaseWrite := findBalance(writes, s.coinbase)
	require.NotNil(t, coinbaseWrite, "coinbase BalancePath write must exist")
	gotCoinbase := coinbaseWrite.Val
	assert.Equal(t, *uint256.NewInt(expectedCoinbase), gotCoinbase,
		"coinbase: TxOut (%d) + tip (%d) = %d expected", postDebitBal, tip, expectedCoinbase)

	burntWrite := findBalance(writes, s.burntAddr)
	require.NotNil(t, burntWrite, "burnt BalancePath write must exist under London")
	gotBurnt := burntWrite.Val
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

		// Stamp each TxOut entry with this iteration's Version before
		// flushing so the writes land at (txIdx, 0). Without this, the
		// scenario baseline's unstamped writes default to (0, 0) and
		// collide with the prior iteration's finalize tip-credit writes
		// at (prevTxIdx, 1) — versionMap.writeLocked panics when the new
		// incarnation is lower than the existing cell's.
		iterVersion := state.Version{BlockNum: 1, TxNum: uint64(txIdx + 1), TxIndex: txIdx, Incarnation: 0}

		// Each tx: TxOut has the scenario baseline (coinbase NoncePath) plus
		// a coinbase BalancePath at postDebitBal. Both are stamped with this
		// iteration's Version so they land at (txIdx, 0). Without the stamp,
		// the scenario baseline's unstamped writes default to (0, 0) and
		// collide with the prior iteration's finalize tip-credit writes at
		// (prevTxIdx, 1) — versionMap.writeLocked panics when the new
		// incarnation is lower than the existing cell's. Since each tx nets
		// coinbase back to preBlockBal after finalize, the next tx still sees
		// preBlockBal as its base.
		s.txOut = newWS().
			nonce(s.coinbase, iterVersion, uint64(1)).
			bal(s.coinbase, iterVersion, *uint256.NewInt(postDebitBal)).
			build()

		result := s.buildExecResult()
		result.TxIn = copyReadSet(s.txIn)
		result.TxOut = copyWrites(s.txOut)
		result.CollectorWrites = copyWrites(s.collectorWrites)

		// Set this tx's version explicitly so versionMap reads land on
		// the right tx-index for the floor-read semantics.
		task := result.Task.(*taskVersion)
		task.version = iterVersion

		vm.FlushVersionedWrites(result.TxOut, true, "")

		writes, err := result.calcFees(task, vm, reader, s.rules)
		require.NoError(t, err, "tx %d: calcFees", txIdx)

		// Flush finalize writes so the next tx sees them via versionMap.
		vm.FlushVersionedWrites(writes, true, "")

		coinbaseWrite := findBalance(writes, s.coinbase)
		require.NotNil(t, coinbaseWrite, "tx %d: coinbase BalancePath write must exist (workerWroteCoinbase gate fires when newBal==oldBal)", txIdx)
		got := coinbaseWrite.Val
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
	s.txOut.SetBalance(s.coinbase, &state.VersionedWrite[uint256.Int]{
		WriteHeader: state.WriteHeader{Address: s.coinbase, Path: state.BalancePath},
		Val:         *uint256.NewInt(reExecutedPostBal), // the re-executed (correct) value
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
	vm.WriteBalance(s.coinbase,
		state.Version{TxIndex: 0, Incarnation: 0},
		*uint256.NewInt(abandonedPostBal), true)

	// Now flush the re-executed TxOut at incarnation 1.
	vm.FlushVersionedWrites(result.TxOut, true, "")

	writes, err := result.calcFees(task, vm, reader, s.rules)
	require.NoError(t, err)

	coinbaseWrite := findBalance(writes, s.coinbase)
	require.NotNil(t, coinbaseWrite, "coinbase BalancePath write must exist")
	got := coinbaseWrite.Val
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
	writes := s.runFinalizeTx(t, priorBalance)

	w := findBalance(writes, s.coinbase)
	if w == nil {
		t.Fatal("no coinbase BalancePath write found")
	}
	// The task has TxIndex=0 (first TX). Verify the Version matches and is
	// not the zero-value struct.
	assert.Equal(t, 0, w.WriteHeader.Version.TxIndex,
		"coinbase write should have Version from task (txIndex=0)")
	assert.Equal(t, uint64(1), w.WriteHeader.Version.BlockNum,
		"coinbase write should have correct BlockNum")
}

// TestFinalizeTxSimple_LondonBurntFees verifies that FeeBurnt is added to
// the burnt contract balance.
func TestFinalizeTxSimple_LondonBurntFees(t *testing.T) {
	t.Parallel()
	s := londonTransferScenario()
	priorBalance := uint256.NewInt(1_000_000_000_000_000_000)
	writes := s.runFinalizeTx(t, priorBalance)

	burntWrite := findBalance(writes, s.burntAddr)
	require.NotNil(t, burntWrite, "finalize should produce burnt contract BalancePath write")

	burntBalance := burntWrite.Val
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
	writes := s.runFinalizeTx(t, nil)

	coinbaseWrite := findBalance(writes, s.coinbase)
	require.NotNil(t, coinbaseWrite, "finalize should produce coinbase BalancePath write")

	coinbaseBalance := coinbaseWrite.Val
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

		writes, err := result.calcFees(task, vm, reader, s.rules)
		require.NoError(t, err)

		// Flush finalize writes to versionMap for next TX.
		vm.FlushVersionedWrites(writes, true, "")

		// Verify accumulated balance.
		coinbaseWrite := findBalance(writes, s.coinbase)
		require.NotNil(t, coinbaseWrite, "TX %d: should have coinbase write", txIdx)
		bal := coinbaseWrite.Val
		expectedBal := new(uint256.Int).Mul(tipPerTx, uint256.NewInt(uint64(txIdx)))
		assert.Equal(t, *expectedBal, bal,
			"TX %d: coinbase should have accumulated %d tips", txIdx, txIdx)
	}
}

// TestFinalizeTxSimple_FeeWriteInvalidatesStaleCoinbaseRead exercises the
// fix for the parallel-exec lost-coinbase-fee race. The apply-loop tip
// credit (calcFees) writes coinbase BalancePath BEFORE validate runs.
// Under the post-#21387 architecture calcFees stamps at the worker's own
// incarnation (not incarnation+1 as in the pre-#21017 code); invalidation
// of a stale dependent read flows through the value-tiebreaker at validate
// time (versionmap.go's StorageRead-with-Done-entry branch). A reader that
// recorded the pre-tip baseline via stateReader sees the versionMap now
// holds the post-tip value, and validate marks the read invalid so the
// dependent tx re-executes against the post-fee balance.
func TestFinalizeTxSimple_FeeWriteInvalidatesStaleCoinbaseRead(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()

	writes := s.runFinalizeTx(t, nil)
	coinbaseWrite := findBalance(writes, s.coinbase)
	require.NotNil(t, coinbaseWrite, "calcFees should produce a coinbase BalancePath write")

	require.Equal(t, 0, coinbaseWrite.WriteHeader.Version.TxIndex)
	require.Equal(t, 0, coinbaseWrite.WriteHeader.Version.Incarnation,
		"calcFees stamps coinbase write at worker incarnation (no +1 bump under post-#21387 architecture)")

	// Reflect calcFees in the versionMap.
	vm := state.NewVersionMap(nil)
	cbWS := &state.WriteSet{}
	cbWS.SetBalance(s.coinbase, coinbaseWrite)
	vm.FlushVersionedWrites(cbWS, true, "")

	checkVersion := func(readV, writeV state.Version) state.VersionValidity {
		if readV != writeV {
			return state.VersionInvalid
		}
		return state.VersionValid
	}

	// validateCoinbaseRead validates a dependent tx (txIndex 1) whose only
	// recorded read is the coinbase balance.
	validateCoinbaseRead := func(source state.ReadSource, readVal uint256.Int) state.VersionValidity {
		io := state.NewVersionedIO(2)
		rs := state.ReadSet{}
		rs.SetBalance(s.coinbase, state.VersionedRead[uint256.Int]{
			ReadHeader: state.ReadHeader{
				Source:  source,
				Version: state.Version{TxIndex: 0, Incarnation: 0},
			},
			Val: readVal,
		})
		io.RecordReads(state.Version{TxIndex: 1}, rs)
		return vm.ValidateVersion(1, io, checkVersion, true, false, "")
	}

	// Stale timing — the dependent worker read the coinbase via stateReader
	// before tx 0's calcFees ran, recording the pre-tip baseline (zero).
	// Validate's value-tiebreaker catches the mismatch.
	assert.Equal(t, state.VersionInvalid, validateCoinbaseRead(state.StorageRead, uint256.Int{}),
		"a stale read of pre-tip baseline (via stateReader) must be invalidated by validate's value-tiebreaker")

	// Late timing — the dependent worker read after calcFees, recording the
	// post-tip value at the worker's version (MapRead). Validate passes.
	coinbaseVal := coinbaseWrite.Val
	assert.Equal(t, state.VersionValid, validateCoinbaseRead(state.MapRead, coinbaseVal),
		"a fresh read of post-tip value at the same version must stay valid")
}

// TestFinalizeTxSimple_BurntFeeWriteStampsWorkerIncarnation verifies the
// burnt contract's implicit FeeBurnt write is stamped at the worker's own
// incarnation (same as coinbase under the post-#21387 architecture).
func TestFinalizeTxSimple_BurntFeeWriteStampsWorkerIncarnation(t *testing.T) {
	t.Parallel()
	s := londonTransferScenario()

	writes := s.runFinalizeTx(t, nil)
	burntWrite := findBalance(writes, s.burntAddr)
	require.NotNil(t, burntWrite, "calcFees should produce a burnt contract BalancePath write")

	assert.Equal(t, 0, burntWrite.WriteHeader.Version.TxIndex)
	assert.Equal(t, 0, burntWrite.WriteHeader.Version.Incarnation,
		"calcFees stamps burnt write at worker incarnation (no +1 bump under post-#21387 architecture)")
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
	vm.FlushVersionedWrites(newWS().stor(addr, slotA, state.Version{TxIndex: 0, Incarnation: 0}, val100).build(), true, "")

	// TX 1 writes slotA=100 (same as TX 0 — no-op)
	writeSet := newWS().stor(addr, slotA, state.Version{TxIndex: 1, Incarnation: 0}, val100).build()
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 1, 0, nil, nil, true, false)

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
	vm.FlushVersionedWrites(newWS().stor(addr, slotA, state.Version{TxIndex: 0, Incarnation: 0}, val100).build(), true, "")

	// TX 1 writes slotA=200 (changed from TX 0's 100)
	writeSet := newWS().stor(addr, slotA, state.Version{TxIndex: 1, Incarnation: 0}, val200).build()
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 1, 0, nil, nil, true, false)

	storageCount := countPath(result, state.StoragePath)
	assert.Equal(t, 1, storageCount, "changed storage write should be kept")
	if w, ok := result.GetStorage(addr, slotA); ok {
		assert.Equal(t, val200, w.Val, "should have resolved value 200")
	}
}

// Case 1c: No prior TX wrote slot — new key, should be kept.
func TestNormalizeWriteSet_StorageNewKey(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x12})
	slotA := accounts.InternKey([32]byte{0x01})

	val100 := *uint256.NewInt(100)

	// TX 0 writes slotA=100 (no prior TX)
	writeSet := newWS().stor(addr, slotA, state.Version{TxIndex: 0, Incarnation: 0}, val100).build()
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, nil, nil, true, false)

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
	vm.FlushVersionedWrites(newWS().
		stor(addr, slotA, state.Version{TxIndex: 5, Incarnation: 0}, val100).
		stor(addr, slotB, state.Version{TxIndex: 5, Incarnation: 0}, val200).
		build(), true, "")

	// TX 5 incarnation 1: only writes slotA=100
	inc1Writes := newWS().stor(addr, slotA, state.Version{TxIndex: 5, Incarnation: 1}, val100).build()
	vm.FlushVersionedWrites(inc1Writes, true, "")

	// The WriteSet has BOTH incarnation 0 and 1 entries (versionMap doesn't clear old)
	// But we pass incarnation=1 as the validated one
	allWrites := newWS().
		stor(addr, slotA, state.Version{TxIndex: 5, Incarnation: 1}, val100).
		stor(addr, slotB, state.Version{TxIndex: 5, Incarnation: 0}, val200). // stale
		build()

	result := normalizeWriteSet(allWrites, vm, 5, 1, nil, nil, true, false)

	storageCount := countPath(result, state.StoragePath)
	assert.Equal(t, 1, storageCount, "only incarnation 1's slotA should survive")
	if storageCount > 0 {
		_, ok := result.GetStorage(addr, slotA)
		assert.True(t, ok, "the surviving storage write must be slotA")
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
	vm.FlushVersionedWrites(newWS().
		stor(addr, slotA, state.Version{TxIndex: 0, Incarnation: 0}, val100).
		stor(addr, slotB, state.Version{TxIndex: 0, Incarnation: 0}, val200).
		build(), true, "")

	// TX 1 self-destructs (val=true means actually destructed)
	writeSet := newWS().selfDestruct(addr, state.Version{TxIndex: 1, Incarnation: 0}, true).build()
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 1, 0, nil, nil, true, false)

	// Should have: SelfDestructPath + DELETE for slotA + DELETE for slotB
	sdCount := countPath(result, state.SelfDestructPath)
	storageCount := countPath(result, state.StoragePath)

	assert.Equal(t, 1, sdCount, "should have SelfDestructPath entry")
	assert.Equal(t, 2, storageCount, "should have DELETE entries for both slots")

	// Verify DELETEs have zero values
	for _, inner := range result.Storages() {
		for _, w := range inner {
			assert.True(t, w.Val.IsZero(), "self-destruct storage DELETE should have zero value")
		}
	}
}

// Case 4: Account field resolution from versionMap.
func TestNormalizeWriteSet_AccountFieldResolution(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x15})

	// TX 0 writes balance=100
	vm.FlushVersionedWrites(newWS().bal(addr, state.Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(100)).build(), true, "")

	// TX 1 writes balance=150 (accumulated from TX 0's 100 + delta)
	vm.FlushVersionedWrites(newWS().bal(addr, state.Version{TxIndex: 1, Incarnation: 0}, *uint256.NewInt(150)).build(), true, "")

	// Worker's WriteSet had stale balance=120 (from speculative execution)
	writeSet := newWS().bal(addr, state.Version{TxIndex: 1, Incarnation: 0}, *uint256.NewInt(120)).build()

	result := normalizeWriteSet(writeSet, vm, 1, 0, nil, nil, true, false)

	require.Equal(t, 1, writeSetLen(result))
	w, ok := result.GetBalance(addr)
	require.True(t, ok)
	assert.Equal(t, *uint256.NewInt(150), w.Val,
		"balance should be resolved from versionMap (150), not worker's stale value (120)")
}

// Case 5: AddressPath entries are excluded.
func TestNormalizeWriteSet_AddressPathExcluded(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x16})

	writeSet := newWS().
		addr(addr, state.Version{TxIndex: 0, Incarnation: 0}, &accounts.Account{}).
		bal(addr, state.Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(100)).
		build()
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, nil, nil, true, false)

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
	writeSet := newWS().stor(addr, slotA, state.Version{TxIndex: 0, Incarnation: 0}, val100).build()
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, reader, nil, true, false)

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
	vm.FlushVersionedWrites(newWS().stor(addr, slotA, state.Version{TxIndex: 0, Incarnation: 0}, val100).build(), true, "")

	// TX 1 writes slotA=100 (same as TX 0 — no-op, will be filtered)
	writeSet := newWS().stor(addr, slotA, state.Version{TxIndex: 1, Incarnation: 0}, val100).build()
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 1, 0, reader, nil, true, false)

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
	ver0 := state.Version{TxIndex: 0, Incarnation: 0}
	writeSet := newWS().
		createContract(addr, ver0, true).
		bal(addr, ver0, *uint256.NewInt(0)).
		nonce(addr, ver0, uint64(1)).
		inc(addr, ver0, uint64(1)).
		codeHash(addr, ver0, codeHash).
		build()
	vm.FlushVersionedWrites(writeSet, true, "")

	result := normalizeWriteSet(writeSet, vm, 0, 0, nil, nil, true, false)

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
	writeSet := newWS().bal(addr, state.Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(50000)).build()
	vm.FlushVersionedWrites(writeSet, true, "")

	// stateReader returns nil for this address (doesn't exist yet)
	reader := newMapStateReader() // empty — no accounts

	result := normalizeWriteSet(writeSet, vm, 0, 0, reader, nil, true, false)

	balCount := countPath(result, state.BalancePath)
	nonceCount := countPath(result, state.NoncePath)
	codeHashCount := countPath(result, state.CodeHashPath)

	assert.Equal(t, 1, balCount, "balance should be present")
	assert.Equal(t, 1, nonceCount, "nonce=0 should be emitted for new account")
	assert.Equal(t, 1, codeHashCount, "empty codeHash should be emitted for new account")

	// Verify nonce is 0 (not missing)
	if w, ok := result.GetNonce(addr); ok {
		assert.Equal(t, uint64(0), w.Val, "nonce should be 0")
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
	ver5 := state.Version{TxIndex: 5, Incarnation: 0}
	writeSet := newWS().
		bal(addr, ver5, *uint256.NewInt(0)).
		nonce(addr, ver5, uint64(0)).
		codeHash(addr, ver5, emptyCodeHash).
		build()
	vm.FlushVersionedWrites(writeSet, true, "")

	// The stateReader has the account from the prior block (Balance > 0).
	reader := newMapStateReader()
	reader.accounts[addr] = &accounts.Account{
		Balance:  *uint256.NewInt(1400000000000000),
		Nonce:    2,
		CodeHash: emptyCodeHash,
	}

	result := normalizeWriteSet(writeSet, vm, 5, 0, reader, nil, true, false)

	// The normalized output should produce a Delete for this account,
	// NOT a regular write with Balance=0, Nonce=0.
	// Serial's updateAccount checks: EIP161Enabled && stateObject.data.Empty()
	// → calls DeleteAccount → emits SelfDestructPath=true

	hasDelete := false
	hasNonDeleteAccount := false
	if w, ok := result.GetSelfDestruct(addr); ok && w.Val {
		hasDelete = true
	}
	if _, ok := result.GetBalance(addr); ok {
		hasNonDeleteAccount = true
	}
	if _, ok := result.GetNonce(addr); ok {
		hasNonDeleteAccount = true
	}
	if _, ok := result.GetCodeHash(addr); ok {
		hasNonDeleteAccount = true
	}

	assert.True(t, hasDelete,
		"empty account (Balance=0, Nonce=0, empty CodeHash) should be deleted via EIP-161")
	assert.False(t, hasNonDeleteAccount,
		"deleted account should NOT have regular account field writes")
}

// Case 10b: AuRa retains its SystemAddress (0xff…fe) even when empty.
// The reference AuRa implementation exempts the SystemAddress from EIP-161
// empty-account removal, so normalizeWriteSet must not turn it into a delete on
// an AuRa chain — while a non-AuRa chain still removes it like any other empty
// account.
func TestNormalizeWriteSet_AuraSystemAddressRetained(t *testing.T) {
	emptyCodeHash := accounts.InternCodeHash(common.HexToHash(
		"c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"))

	run := func(isAura bool) *state.WriteSet {
		vm := state.NewVersionMap(nil)
		ver := state.Version{TxIndex: 5, Incarnation: 0}
		writeSet := newWS().
			bal(params.SystemAddress, ver, *uint256.NewInt(0)).
			nonce(params.SystemAddress, ver, uint64(0)).
			codeHash(params.SystemAddress, ver, emptyCodeHash).
			build()
		vm.FlushVersionedWrites(writeSet, true, "")

		reader := newMapStateReader()
		reader.accounts[params.SystemAddress] = &accounts.Account{
			Balance:  *uint256.NewInt(1400000000000000),
			Nonce:    2,
			CodeHash: emptyCodeHash,
		}

		return normalizeWriteSet(writeSet, vm, 5, 0, reader, nil, true, isAura)
	}

	hasDelete := func(writes *state.WriteSet) bool {
		if w, ok := writes.GetSelfDestruct(params.SystemAddress); ok && w.Val {
			return true
		}
		return false
	}

	assert.False(t, hasDelete(run(true)),
		"AuRa SystemAddress must be retained even when empty")
	assert.True(t, hasDelete(run(false)),
		"non-AuRa chain removes an empty account, including the system address")
}

// Pins that normalizeWriteSet recovers CodePath alongside CodeHashPath for a
// 7702 designator, so an account is never left with a codeHash but no code.
func TestNormalizeWriteSet_CodePathTravelsWithCodeHash(t *testing.T) {
	vm := state.NewVersionMap(nil)
	authority := accounts.InternAddress([20]byte{0x42})

	// EIP-7702 delegation designator: 0xef0100 || target(20 bytes).
	designator := types.AddressToDelegation(accounts.InternAddress([20]byte{0x69, 0x00, 0x77, 0x02}))
	designatorHash := accounts.InternCodeHash(crypto.Keccak256Hash(designator))
	designatorCode := accounts.Code{Hash: designatorHash, Bytes: designator}

	const txIndex = 5
	ver0 := state.Version{TxIndex: txIndex, Incarnation: 0}
	ver1 := state.Version{TxIndex: txIndex, Incarnation: 1}

	// Incarnation 0 delegates: designator code + its hash + authority nonce bump.
	vm.FlushVersionedWrites(newWS().
		code(authority, ver0, designatorCode).
		codeHash(authority, ver0, designatorHash).
		nonce(authority, ver0, 1).
		build(), true, "")

	// Incarnation 1 (validated) re-executes; SetCode short-circuits, so only the
	// nonce is re-emitted — no fresh CodePath/CodeHashPath.
	vm.FlushVersionedWrites(newWS().nonce(authority, ver1, 1).build(), true, "")

	// blockIO.WriteSet retains both incarnations' entries (versionMap doesn't
	// clear old), so the validated tx's raw writeset carries the stale inc-0
	// code writes alongside the inc-1 nonce.
	rawWrites := newWS().
		nonce(authority, ver1, 1).
		code(authority, ver0, designatorCode).
		codeHash(authority, ver0, designatorHash).
		build()

	result := normalizeWriteSet(rawWrites, vm, txIndex, 1, nil, nil, true, false)

	gotHash, okHash := result.GetCodeHash(authority)
	require.True(t, okHash, "codeHash must be present in the normalized writeset")
	assert.Equal(t, designatorHash, gotHash.Val, "codeHash is the 7702 designator hash")

	// The regression: code was dropped while the hash survived. Code must travel
	// with its hash so the account is never persisted with a codeHash but no code.
	require.Equal(t, 1, countPath(result, state.CodePath),
		"CodePath must be recovered so code is never persisted without its codeHash")
	gotCode, okCode := result.GetCode(authority)
	require.True(t, okCode)
	assert.Equal(t, designator, gotCode.Val.Bytes, "recovered code is the 7702 designator bytes")
}

// The SetCode short-circuit variant: the designator is already committed (so a
// re-delegating tx's SetCode short-circuits and the versionMap holds NO
// CodePath for this tx at all). The fill-missing loop still fills CodeHashPath
// from committed state, so recovery must fall back to stateReader.ReadAccountCode
// — the versionMap path alone (the original fix) would miss this and persist a
// codeHash with no code.
func TestNormalizeWriteSet_CodePathRecoveredFromStateReader(t *testing.T) {
	vm := state.NewVersionMap(nil)
	authority := accounts.InternAddress([20]byte{0x42})
	designator := types.AddressToDelegation(accounts.InternAddress([20]byte{0x69, 0x00, 0x77, 0x02}))
	designatorHash := accounts.InternCodeHash(crypto.Keccak256Hash(designator))

	const txIndex = 5
	ver0 := state.Version{TxIndex: txIndex, Incarnation: 0}

	// authority is an already-committed 7702 delegation: its designator code +
	// codeHash live in committed state, NOT in this batch's versionMap.
	reader := newMapStateReader()
	reader.accounts[authority] = &accounts.Account{Nonce: 1, CodeHash: designatorHash}
	reader.code[authority] = designator

	// Re-delegating tx: SetCode short-circuits (code unchanged), so the only
	// write is the nonce bump — no CodePath/CodeHashPath, and nothing for
	// CodePath in the versionMap.
	vm.FlushVersionedWrites(newWS().nonce(authority, ver0, 2).build(), true, "")
	rawWrites := newWS().nonce(authority, ver0, 2).build()

	result := normalizeWriteSet(rawWrites, vm, txIndex, 0, reader, nil, true, false)

	require.Equal(t, 1, countPath(result, state.CodeHashPath),
		"codeHash is filled from committed state for the modified account")
	require.Equal(t, 1, countPath(result, state.CodePath),
		"CodePath must be recovered via stateReader when the versionMap has none")
	gotCode, ok := result.GetCode(authority)
	require.True(t, ok)
	assert.Equal(t, designator, gotCode.Val.Bytes, "recovered code is the committed designator")
}

// Characterization tests below pin normalizeWriteSet branches that were only
// covered end-to-end (execmodule), ahead of a planned loop-rationalization
// refactor. They assert current behavior; a refactor must keep them green.

// Metamorphic same-tx SELFDESTRUCT-then-CREATE2: the final SelfDestructPath is
// false (account ends alive), so the address is NOT treated as self-destructed
// and its recreate-time field writes survive — no delete, no storage cascade.
func TestNormalizeWriteSet_MetamorphicSameTxRecreateKeepsWrites(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x30})

	ws := newWS().
		selfDestruct(addr, state.Version{TxIndex: 1, Incarnation: 0}, false). // ended alive
		bal(addr, state.Version{TxIndex: 1, Incarnation: 0}, *uint256.NewInt(500)).
		nonce(addr, state.Version{TxIndex: 1, Incarnation: 0}, 1).
		build()
	vm.FlushVersionedWrites(ws, true, "")

	result := normalizeWriteSet(ws, vm, 1, 0, nil, nil, true, false)

	assert.Equal(t, 0, countPath(result, state.SelfDestructPath), "SelfDestructPath=false is not emitted")
	assert.Equal(t, 0, countPath(result, state.StoragePath), "no storage-delete cascade for an alive account")
	b, ok := result.GetBalance(addr)
	require.True(t, ok, "recreate balance must survive")
	assert.Equal(t, *uint256.NewInt(500), b.Val)
	n, ok := result.GetNonce(addr)
	require.True(t, ok, "recreate nonce must survive")
	assert.Equal(t, uint64(1), n.Val)
}

// A SelfDestructPath=true from a NON-validated incarnation must not mark the
// address self-destructed: the validated incarnation's field writes survive and
// no delete/cascade is emitted.
func TestNormalizeWriteSet_StaleIncarnationSelfDestructIgnored(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x31})

	ws := newWS().
		selfDestruct(addr, state.Version{TxIndex: 5, Incarnation: 0}, true). // stale incarnation
		bal(addr, state.Version{TxIndex: 5, Incarnation: 1}, *uint256.NewInt(300)).
		build()
	vm.FlushVersionedWrites(ws, true, "")

	result := normalizeWriteSet(ws, vm, 5, 1, nil, nil, true, false)

	assert.Equal(t, 0, countPath(result, state.SelfDestructPath), "stale-incarnation SD must not be emitted")
	b, ok := result.GetBalance(addr)
	require.True(t, ok, "validated-incarnation balance must survive (not dropped as SD)")
	assert.Equal(t, *uint256.NewInt(300), b.Val)
}

// History-scan no-op: after an earlier tx self-destructed the address (even if a
// later tx revived it, so the latest SelfDestructPath is false), a post-SD write
// of zero is dropped because the SD cascade already fixed the baseline at zero.
func TestNormalizeWriteSet_PostSelfDestructZeroStorageDroppedViaHistory(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x32})
	slot := accounts.InternKey([32]byte{0x01})

	// tx2 destructs, tx3 revives → latest SelfDestructPath is false, but history
	// carries a Done true at tx2.
	vm.FlushVersionedWrites(newWS().selfDestruct(addr, state.Version{TxIndex: 2, Incarnation: 0}, true).build(), true, "")
	vm.FlushVersionedWrites(newWS().selfDestruct(addr, state.Version{TxIndex: 3, Incarnation: 0}, false).build(), true, "")
	reader := newMapStateReader()

	zeroWrite := newWS().stor(addr, slot, state.Version{TxIndex: 5, Incarnation: 0}, uint256.Int{}).build()
	dropped := normalizeWriteSet(zeroWrite, vm, 5, 0, reader, nil, true, false)
	assert.Equal(t, 0, countPath(dropped, state.StoragePath), "post-SD zero write is a no-op against the zero baseline")

	// Control: a non-zero post-SD write is a real change and survives.
	nonZero := newWS().stor(addr, slot, state.Version{TxIndex: 5, Incarnation: 0}, *uint256.NewInt(77)).build()
	kept := normalizeWriteSet(nonZero, vm, 5, 0, reader, nil, true, false)
	s, ok := kept.GetStorage(addr, slot)
	require.True(t, ok, "non-zero post-SD write must survive")
	assert.Equal(t, *uint256.NewInt(77), s.Val)
}

// Backfill after an earlier-tx self-destruct: when THIS tx re-creates via
// CREATE(2) (CreateContractPath=true), the missing account fields are the
// post-destruction zero defaults — NOT the stale pre-SD nonce/codeHash still in
// the versionMap. A value-transfer resurrect (no CreateContractPath) instead
// inherits the pre-SD fields via the map's last-write-wins chain.
func TestNormalizeWriteSet_SelfDestructEarlierThenCreateContractZeroesFields(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x33})
	staleHash := accounts.InternCodeHash(common.HexToHash("0x11223344"))

	// Pre-SD stale fields, then a self-destruct that is the latest SD state.
	vm.FlushVersionedWrites(newWS().
		nonce(addr, state.Version{TxIndex: 1, Incarnation: 0}, 9).
		codeHash(addr, state.Version{TxIndex: 1, Incarnation: 0}, staleHash).
		build(), true, "")
	vm.FlushVersionedWrites(newWS().selfDestruct(addr, state.Version{TxIndex: 2, Incarnation: 0}, true).build(), true, "")

	// tx5 re-creates via CREATE2 and funds it (balance keeps the account
	// non-empty so EIP-161 doesn't delete it and the zeroed fields are visible).
	created := newWS().
		createContract(addr, state.Version{TxIndex: 5, Incarnation: 0}, true).
		bal(addr, state.Version{TxIndex: 5, Incarnation: 0}, *uint256.NewInt(100)).
		build()
	res := normalizeWriteSet(created, vm, 5, 0, nil, nil, true, false)
	n, ok := res.GetNonce(addr)
	require.True(t, ok)
	assert.Equal(t, uint64(0), n.Val, "CREATE2 after SD gets zero nonce, not the stale pre-SD 9")
	ch, ok := res.GetCodeHash(addr)
	require.True(t, ok)
	assert.Equal(t, accounts.EmptyCodeHash, ch.Val, "CREATE2 after SD gets empty codeHash, not the stale pre-SD hash")

	// Control: value-transfer resurrect (no CreateContractPath) inherits pre-SD
	// fields from the versionMap.
	resurrect := newWS().bal(addr, state.Version{TxIndex: 5, Incarnation: 0}, *uint256.NewInt(100)).build()
	res2 := normalizeWriteSet(resurrect, vm, 5, 0, nil, nil, true, false)
	n2, ok := res2.GetNonce(addr)
	require.True(t, ok)
	assert.Equal(t, uint64(9), n2.Val, "value-transfer resurrect inherits the pre-SD nonce")
}

// Storage no-op detected via the stateReader's pre-block value (no prior in-block
// write in the versionMap and no self-destruct history): a write-back of the
// pre-block value is dropped; a different value survives.
func TestNormalizeWriteSet_StorageNoOpViaStateReaderPreBlock(t *testing.T) {
	vm := state.NewVersionMap(nil)
	addr := accounts.InternAddress([20]byte{0x34})
	slot := accounts.InternKey([32]byte{0x01})

	reader := newMapStateReader()
	reader.storage[addr] = map[accounts.StorageKey]uint256.Int{slot: *uint256.NewInt(100)}

	sameVal := newWS().stor(addr, slot, state.Version{TxIndex: 1, Incarnation: 0}, *uint256.NewInt(100)).build()
	dropped := normalizeWriteSet(sameVal, vm, 1, 0, reader, nil, true, false)
	assert.Equal(t, 0, countPath(dropped, state.StoragePath), "write-back of the pre-block value is a no-op")

	diffVal := newWS().stor(addr, slot, state.Version{TxIndex: 1, Incarnation: 0}, *uint256.NewInt(200)).build()
	kept := normalizeWriteSet(diffVal, vm, 1, 0, reader, nil, true, false)
	s, ok := kept.GetStorage(addr, slot)
	require.True(t, ok, "a changed value must survive")
	assert.Equal(t, *uint256.NewInt(200), s.Val)
}

// TestCalcFees_EmitsAddressPathForCoinbase pins that calcFees emits an
// AddressPath sibling alongside the coinbase BalancePath credit, mirroring
// serial's create-on-first-credit so a later parallel tx's getVersionedAccount
// sees the account exists (else Empty(coinbase)=true wrongly charges
// CallNewAccountGas). The AddressPath account's Balance must match BalancePath.
func TestCalcFees_EmitsAddressPathForCoinbase(t *testing.T) {
	t.Parallel()
	s := simpleTransferScenario()

	// No prior coinbase balance — first credit of the block.
	writes := s.runFinalizeTx(t, nil)

	coinbaseBalance := findBalance(writes, s.coinbase)
	require.NotNil(t, coinbaseBalance,
		"calcFees should emit coinbase BalancePath write")
	require.Equal(t, s.feeTipped, coinbaseBalance.Val,
		"BalancePath value must equal feeTipped (no prior balance)")

	coinbaseAddress := findAddress(writes, s.coinbase)
	require.NotNil(t, coinbaseAddress,
		"calcFees MUST emit coinbase AddressPath sibling write so "+
			"downstream parallel txs see the account record (mainnet "+
			"25151825 tx 31 SD+CREATE2-on-coinbase +25k regression pin)")

	addrAcc := coinbaseAddress.Val
	require.NotNil(t, addrAcc, "AddressPath account must not be nil")
	require.Equal(t, s.feeTipped, addrAcc.Balance,
		"AddressPath account.Balance must equal the BalancePath value, "+
			"otherwise downstream getVersionedAccount returns a stale balance")
	require.True(t, addrAcc.IsEmptyCodeHash(),
		"freshly-created coinbase has empty code (no pre-block contract)")
	require.Equal(t, coinbaseBalance.WriteHeader.Version, coinbaseAddress.WriteHeader.Version,
		"AddressPath sibling must share version with the BalancePath write")
}
