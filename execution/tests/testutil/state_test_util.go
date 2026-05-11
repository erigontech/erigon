// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package testutil

import (
	"context"
	context2 "context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// StateTest checks transaction processing without block context.
// See https://github.com/ethereum/EIPs/issues/176 for the test format specification.
type StateTest struct {
	Json stJSON
}

// StateSubtest selects a specific configuration of a General State Test.
type StateSubtest struct {
	Fork  string
	Index int
}

func (t *StateTest) UnmarshalJSON(in []byte) error {
	return jsoniter.ConfigFastest.Unmarshal(in, &t.Json)
}

type stJSON struct {
	Env  stEnv                    `json:"env"`
	Pre  types.GenesisAlloc       `json:"pre"`
	Tx   stTransaction            `json:"transaction"`
	Out  hexutil.Bytes            `json:"out"`
	Post map[string][]stPostState `json:"post"`
}

type stPostState struct {
	Root            common.UnprefixedHash `json:"hash"`
	Logs            common.UnprefixedHash `json:"logs"`
	Tx              hexutil.Bytes         `json:"txbytes"`
	ExpectException string                `json:"expectException"`
	Indexes         struct {
		Data  int `json:"data"`
		Gas   int `json:"gas"`
		Value int `json:"value"`
	}
}

type stTransaction struct {
	GasPrice             *math.HexOrDecimal256 `json:"gasPrice"`
	MaxFeePerGas         *math.HexOrDecimal256 `json:"maxFeePerGas"`
	MaxPriorityFeePerGas *math.HexOrDecimal256 `json:"maxPriorityFeePerGas"`
	Nonce                math.HexOrDecimal64   `json:"nonce"`
	GasLimit             []math.HexOrDecimal64 `json:"gasLimit"`
	PrivateKey           hexutil.Bytes         `json:"secretKey"`
	To                   string                `json:"to"`
	Data                 []string              `json:"data"`
	Value                []string              `json:"value"`
	AccessLists          []*types.AccessList   `json:"accessLists,omitempty"`
	BlobVersionedHashes  []common.Hash         `json:"blobVersionedHashes,omitempty"`
	BlobGasFeeCap        *math.HexOrDecimal256 `json:"maxFeePerBlobGas,omitempty"`
	Authorizations       []stAuthorization     `json:"authorizationList"`
	IsSetCodeTx          bool                  `json:"-"` // true when authorizationList present in JSON
}

func (tx *stTransaction) UnmarshalJSON(data []byte) error {
	// First check if authorizationList is present in the raw JSON
	var raw map[string]jsoniter.RawMessage
	if err := jsoniter.ConfigFastest.Unmarshal(data, &raw); err != nil {
		return err
	}
	if _, ok := raw["authorizationList"]; ok {
		tx.IsSetCodeTx = true
	}
	// Then unmarshal normally using an alias to avoid recursion
	type stTransactionAlias stTransaction
	alias := (*stTransactionAlias)(tx)
	return jsoniter.ConfigFastest.Unmarshal(data, alias)
}

// stAuthorization is a test-specific authorization type that accepts "0x00"
// for chainId (which hexutil.Big rejects due to leading zero).
type stAuthorization struct {
	ChainID *math.HexOrDecimal256 `json:"chainId"`
	Address common.Address        `json:"address"`
	Nonce   math.HexOrDecimal64   `json:"nonce"`
	V       math.HexOrDecimal64   `json:"v"`
	R       *math.HexOrDecimal256 `json:"r"`
	S       *math.HexOrDecimal256 `json:"s"`
}

func (a stAuthorization) ToAuthorization() (types.Authorization, error) {
	auth := types.Authorization{
		Address: a.Address,
		Nonce:   uint64(a.Nonce),
	}
	if a.ChainID != nil {
		chainId, overflow := uint256.FromBig((*big.Int)(a.ChainID))
		if overflow {
			return auth, errors.New("chainId does not fit in 256 bits")
		}
		auth.ChainID = *chainId
	}
	auth.YParity = uint8(a.V)
	if a.R != nil {
		r, overflow := uint256.FromBig((*big.Int)(a.R))
		if overflow {
			return auth, errors.New("r does not fit in 256 bits")
		}
		auth.R = *r
	}
	if a.S != nil {
		s, overflow := uint256.FromBig((*big.Int)(a.S))
		if overflow {
			return auth, errors.New("s does not fit in 256 bits")
		}
		auth.S = *s
	}
	return auth, nil
}

//go:generate gencodec -type stEnv -field-override stEnvMarshaling -out gen_stenv.go

type stEnv struct {
	Coinbase      common.Address `json:"currentCoinbase"   gencodec:"required"`
	Difficulty    *uint256.Int   `json:"currentDifficulty" gencodec:"required"`
	Random        *uint256.Int   `json:"currentRandom"     gencodec:"optional"`
	GasLimit      uint64         `json:"currentGasLimit"   gencodec:"required"`
	Number        uint64         `json:"currentNumber"     gencodec:"required"`
	Timestamp     uint64         `json:"currentTimestamp"  gencodec:"required"`
	BaseFee       *uint256.Int   `json:"currentBaseFee"    gencodec:"optional"`
	ExcessBlobGas *uint64        `json:"currentExcessBlobGas" gencodec:"optional"`
}

type stEnvMarshaling struct {
	Coinbase      common.UnprefixedAddress
	Difficulty    *math.HexOrDecimal256
	Random        *math.HexOrDecimal256
	GasLimit      math.HexOrDecimal64
	Number        math.HexOrDecimal64
	Timestamp     math.HexOrDecimal64
	BaseFee       *math.HexOrDecimal256
	ExcessBlobGas *math.HexOrDecimal64
}

// GetChainConfig takes a fork definition and returns a chain config.
// The fork definition can be
// - a plain forkname, e.g. `Byzantium`,
// - a fork basename, and a list of EIPs to enable; e.g. `Byzantium+1884+1283`.
func GetChainConfig(forkString string) (baseConfig *chain.Config, eips []int, err error) {
	var (
		splitForks            = strings.Split(forkString, "+")
		ok                    bool
		baseName, eipsStrings = splitForks[0], splitForks[1:]
	)
	if baseConfig, ok = testforks.Forks[baseName]; !ok {
		return nil, nil, testforks.UnsupportedForkError{Name: baseName}
	}
	for _, eip := range eipsStrings {
		if eipNum, err := strconv.Atoi(eip); err != nil {
			return nil, nil, fmt.Errorf("syntax error, invalid eip number %v", eipNum)
		} else {
			if !vm.ValidEip(eipNum) {
				return nil, nil, fmt.Errorf("syntax error, invalid eip number %v", eipNum)
			}
			eips = append(eips, eipNum)
		}
	}
	return baseConfig, eips, nil
}

// Subtests returns all valid subtests of the test.
func (t *StateTest) Subtests() []StateSubtest {
	totalCount := 0
	for _, pss := range t.Json.Post {
		totalCount += len(pss)
	}
	sub := make([]StateSubtest, 0, totalCount)
	for fork, pss := range t.Json.Post {
		for i := range pss {
			sub = append(sub, StateSubtest{fork, i})
		}
	}
	return sub
}

// checkError checks if the error returned by the state transition matches any expected error.
func (t *StateTest) checkError(subtest StateSubtest, err error) error {
	expectedError := t.Json.Post[subtest.Fork][subtest.Index].ExpectException
	if err == nil && expectedError == "" {
		return nil
	}
	if err == nil && expectedError != "" {
		return fmt.Errorf("expected error %q, got no error", expectedError)
	}
	if err != nil && expectedError == "" {
		return fmt.Errorf("unexpected error: %w", err)
	}
	// err != nil && expectedError != "" — expected error occurred, OK
	return nil
}

// Run executes a specific subtest and verifies the post-state and logs
func (t *StateTest) Run(tb testing.TB, tx kv.TemporalRwTx, subtest StateSubtest, vmconfig vm.Config, dirs datadir.Dirs) (*state.IntraBlockState, common.Hash, error) {
	st, root, _, err := t.RunNoVerify(tb, tx, subtest, vmconfig, dirs)

	checkedErr := t.checkError(subtest, err)
	if checkedErr != nil {
		return st, empty.RootHash, checkedErr
	}
	if err != nil {
		// Error was expected — check post-state root if specified
		post := t.Json.Post[subtest.Fork][subtest.Index]
		if post.Root != (common.UnprefixedHash{}) && root != common.Hash(post.Root) {
			return st, root, fmt.Errorf("post state root mismatch: got %x, want %x", root, post.Root)
		}
		return st, root, nil
	}
	post := t.Json.Post[subtest.Fork][subtest.Index]
	if root != common.Hash(post.Root) {
		return st, root, fmt.Errorf("post state root mismatch: got %x, want %x", root, post.Root)
	}
	if logs := rlpHash(st.Logs()); logs != common.Hash(post.Logs) {
		return st, root, fmt.Errorf("post state logs hash mismatch: got %x, want %x", logs, post.Logs)
	}
	return st, root, nil
}

// RunNoVerify runs a specific subtest and returns the statedb, post-state root and gas used.
func (t *StateTest) RunNoVerify(tb testing.TB, tx kv.TemporalRwTx, subtest StateSubtest, vmconfig vm.Config, dirs datadir.Dirs) (*state.IntraBlockState, common.Hash, uint64, error) {
	config, eips, err := GetChainConfig(subtest.Fork)
	if err != nil {
		return nil, common.Hash{}, 0, testforks.UnsupportedForkError{Name: subtest.Fork}
	}
	vmconfig.ExtraEips = eips
	block, _, err := genesiswrite.GenesisToBlock(nil, t.genesis(config), dirs, log.Root())
	if err != nil {
		return nil, common.Hash{}, 0, testforks.UnsupportedForkError{Name: subtest.Fork}
	}

	readBlockNr := block.NumberU64()
	writeBlockNr := readBlockNr + 1

	_, err = MakePreState(&chain.Rules{}, tx, t.Json.Pre, readBlockNr)
	if err != nil {
		return nil, common.Hash{}, 0, testforks.UnsupportedForkError{Name: subtest.Fork}
	}

	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	if err != nil {
		return nil, common.Hash{}, 0, testforks.UnsupportedForkError{Name: subtest.Fork}
	}
	defer domains.Close()
	blockNum, txNum := readBlockNr, uint64(1)

	r := rpchelper.NewLatestStateReader(tx)
	w := rpchelper.NewLatestStateWriter(tx, domains, (*freezeblocks.BlockReader)(nil), writeBlockNr)
	statedb := state.New(r)

	var baseFee *uint256.Int
	if config.IsLondon(0) {
		baseFee = t.Json.Env.BaseFee
		if baseFee == nil {
			// Retesteth uses `0x10` for genesis baseFee. Therefore, it defaults to
			// parent - 2 : 0xa as the basefee for 'this' context.
			baseFee = uint256.NewInt(0x0a)
		}
	}
	post := t.Json.Post[subtest.Fork][subtest.Index]
	header := block.HeaderNoCopy()
	chainRules := buildStateTestRules(config, header)

	var blobBaseFee *uint256.Int
	if config.IsCancun(block.Time()) && t.Json.Env.ExcessBlobGas != nil {
		bf, err := misc.GetBlobGasPrice(config, *t.Json.Env.ExcessBlobGas, header.Time)
		if err != nil {
			return nil, common.Hash{}, 0, err
		}
		blobBaseFee = &bf
	}

	// EEST fixtures carry the signed RLP-encoded tx in `post.txbytes`; running
	// it through the production AsMessage path means we test real validation
	// (tx-type-vs-fork from each AsMessage, plus EIP-4844 blob structural rules
	// in BlobTx.AsMessage). Decode failures and AsMessage failures both surface
	// here as the tx-apply error: ApplyMessage is skipped but FinalizeTx /
	// CommitBlock still run so the post-state root reflects the unapplied tx.
	//
	// Older in-tree corner-case fixtures only carry the JSON `transaction`
	// block. None of those exercise pre-fork tx-type rejection, so we build
	// the Message via toMessage (no validation) and treat construction errors
	// as fatal.
	var (
		msg      protocol.Message
		applyErr error
	)
	if len(post.Tx) > 0 {
		signer := types.LatestSignerForChainID(config.ChainID)
		signer.SetMalleable(true) // allow Frontier/Homestead malleable signatures
		decodedTx, decodeErr := types.DecodeTransaction(post.Tx)
		if decodeErr != nil {
			applyErr = decodeErr
		} else {
			msg, applyErr = decodedTx.AsMessage(*signer, baseFee, chainRules)
		}
	} else {
		msg, err = toMessage(t.Json.Tx, post, baseFee)
		if err != nil {
			return nil, common.Hash{}, 0, err
		}
	}

	gasUsed := uint64(0)
	if applyErr == nil {
		gasUsed, applyErr = t.applyStateTestMessage(statedb, msg, block, header, baseFee, blobBaseFee, config, vmconfig)
	}

	if err = statedb.FinalizeTx(chainRules, w); err != nil {
		return nil, common.Hash{}, gasUsed, err
	}
	if err = statedb.CommitBlock(chainRules, w); err != nil {
		return nil, common.Hash{}, gasUsed, err
	}

	var root common.Hash
	rootBytes, err := domains.ComputeCommitment(context2.Background(), tx, true, blockNum, txNum, "", nil)
	if err != nil {
		return statedb, root, gasUsed, fmt.Errorf("ComputeCommitment: %w", err)
	}
	// Propagate transaction execution error (e.g. intrinsic gas too low)
	return statedb, common.BytesToHash(rootBytes), gasUsed, applyErr
}

func MakePreState(rules *chain.Rules, tx kv.TemporalRwTx, alloc types.GenesisAlloc, blockNr uint64) (*state.IntraBlockState, error) {
	r := rpchelper.NewLatestStateReader(tx)
	statedb := state.New(r)
	statedb.SetTxContext(blockNr, 0)
	for addr, a := range alloc {
		address := accounts.InternAddress(addr)
		statedb.SetCode(address, a.Code)
		statedb.SetNonce(address, a.Nonce)
		var balance uint256.Int
		if a.Balance != nil {
			_ = balance.SetFromBig(a.Balance)
		}
		statedb.SetBalance(address, balance, tracing.BalanceChangeUnspecified)
		for k, v := range a.Storage {
			key := accounts.InternKey(k)
			val := uint256.NewInt(0).SetBytes(v.Bytes())
			statedb.SetState(address, key, *val)
		}

		if len(a.Code) > 0 || len(a.Storage) > 0 {
			statedb.SetIncarnation(address, state.FirstContractIncarnation)
		}
	}

	domains, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
	if err != nil {
		return nil, err
	}
	defer domains.Close()
	latestTxNum, latestBlockNum, err := domains.SeekCommitment(context.Background(), tx)
	if err != nil {
		return nil, err
	}

	w := rpchelper.NewLatestStateWriter(tx, domains, (*freezeblocks.BlockReader)(nil), blockNr-1)

	// Commit and re-open to start with a clean state.
	if err := statedb.FinalizeTx(rules, w); err != nil {
		return nil, err
	}
	if err := statedb.CommitBlock(rules, w); err != nil {
		return nil, err
	}

	_, err = domains.ComputeCommitment(context.Background(), tx, true, latestBlockNum, latestTxNum, "flush-commitment", nil)
	if err != nil {
		return nil, err
	}

	if err := domains.Flush(context2.Background(), tx); err != nil {
		return nil, err
	}
	return statedb, nil
}

func (t *StateTest) genesis(config *chain.Config) *types.Genesis {
	return &types.Genesis{
		Config:     config,
		Coinbase:   t.Json.Env.Coinbase,
		Difficulty: t.Json.Env.Difficulty,
		GasLimit:   t.Json.Env.GasLimit,
		Number:     t.Json.Env.Number,
		Timestamp:  t.Json.Env.Timestamp,
		Alloc:      t.Json.Pre,
	}
}

var rlpHash = types.RlpHash

func vmTestBlockHash(n uint64) (common.Hash, error) {
	return common.BytesToHash(crypto.Keccak256([]byte(new(big.Int).SetUint64(n).String()))), nil
}

// applyStateTestMessage runs ApplyMessage for a state-test subtest and applies
// the post-apply coinbase touch. The returned error is the ApplyMessage error,
// which the test framework may treat as expected (matched against the fixture's
// ExpectException); it is not a fatal runner error.
//
// This is only called when the message was built successfully. For pre-apply
// rejections (decode/AsMessage failures) the caller skips it: on pre-EIP-158
// forks a stray coinbase touch would create an empty account that never gets
// pruned, breaking the post-state root the fixture expects for an unapplied tx.
func (t *StateTest) applyStateTestMessage(statedb *state.IntraBlockState, msg protocol.Message, block *types.Block, header *types.Header, baseFee, blobBaseFee *uint256.Int, config *chain.Config, vmconfig vm.Config) (uint64, error) {
	txContext := protocol.NewEVMTxContext(msg)
	context := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, nil), nil, accounts.InternAddress(t.Json.Env.Coinbase), config)
	context.GetHash = vmTestBlockHash
	if baseFee != nil {
		context.BaseFee.Set(baseFee)
	}
	if blobBaseFee != nil {
		context.BlobBaseFee = *blobBaseFee
	}
	if t.Json.Env.Difficulty != nil {
		context.Difficulty.Set(t.Json.Env.Difficulty)
	}
	if config.IsLondon(0) && t.Json.Env.Random != nil {
		rnd := common.Hash(t.Json.Env.Random.Bytes32())
		context.PrevRanDao = &rnd
		context.Difficulty.Clear()
	}
	evm := vm.NewEVM(context, txContext, statedb, config, vmconfig)
	if vmconfig.Tracer != nil && vmconfig.Tracer.OnTxStart != nil {
		vmconfig.Tracer.OnTxStart(evm.GetVMContext(), nil, accounts.ZeroAddress)
	}

	snapshot := statedb.PushSnapshot()
	gaspool := new(protocol.GasPool)
	gaspool.AddGas(block.GasLimit()).AddBlobGas(config.GetMaxBlobGasPerBlock(header.Time))
	res, applyErr := protocol.ApplyMessage(evm, msg, gaspool, true /* refunds */, false /* gasBailout */, nil /* engine */)
	gasUsed := uint64(0)
	if res != nil {
		gasUsed = res.ReceiptGasUsed
	}
	if applyErr != nil {
		statedb.RevertToSnapshot(snapshot, applyErr)
	}
	statedb.PopSnapshot(snapshot)
	if vmconfig.Tracer != nil && vmconfig.Tracer.OnTxEnd != nil {
		vmconfig.Tracer.OnTxEnd(&types.Receipt{GasUsed: gasUsed}, nil)
	}
	statedb.AddBalance(accounts.InternAddress(t.Json.Env.Coinbase), *uint256.NewInt(0), tracing.BalanceChangeUnspecified)
	return gasUsed, applyErr
}

// buildStateTestRules constructs a chain.Rules for the given state-test header.
// State tests run a single tx against genesis, so we don't have a full EVM/block
// context handy when we need rules outside of the apply path (e.g. for
// FinalizeTx/CommitBlock when the tx is rejected pre-apply).
func buildStateTestRules(c *chain.Config, header *types.Header) *chain.Rules {
	num := header.Number.Uint64()
	t := header.Time
	chainID := c.ChainID
	if chainID == nil {
		chainID = new(big.Int)
	}
	return &chain.Rules{
		ChainID:            new(big.Int).Set(chainID),
		IsHomestead:        c.IsHomestead(num),
		IsTangerineWhistle: c.IsTangerineWhistle(num),
		IsSpuriousDragon:   c.IsSpuriousDragon(num),
		IsByzantium:        c.IsByzantium(num),
		IsConstantinople:   c.IsConstantinople(num),
		IsPetersburg:       c.IsPetersburg(num),
		IsIstanbul:         c.IsIstanbul(num),
		IsBerlin:           c.IsBerlin(num),
		IsLondon:           c.IsLondon(num),
		IsShanghai:         c.IsShanghai(t),
		IsCancun:           c.IsCancun(t),
		IsPrague:           c.IsPrague(t),
		IsOsaka:            c.IsOsaka(t),
		IsAmsterdam:        c.IsAmsterdam(t),
	}
}

// toMessage builds a protocol.Message directly from the JSON fixture's
// `transaction` block. It is only used for the in-tree corner-case fixtures
// that don't carry signed `txbytes` — EEST fixtures go through
// types.DecodeTransaction + AsMessage instead, which exercises the production
// validation path. toMessage itself does no fork-vs-tx-type validation;
// fixtures that depend on this fallback aren't testing that.
func toMessage(tx stTransaction, ps stPostState, baseFee *uint256.Int) (protocol.Message, error) {
	// Derive sender from private key if present.
	var from accounts.Address
	if len(tx.PrivateKey) > 0 {
		key, err := crypto.ToECDSA(tx.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("invalid private key: %v", err)
		}
		from = accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
	}

	// Parse recipient if present.
	var to accounts.Address
	if tx.To != "" {
		var txto common.Address
		if err := txto.UnmarshalText([]byte(tx.To)); err != nil {
			return nil, fmt.Errorf("invalid to address: %v", err)
		}
		to = accounts.InternAddress(txto)
	}

	// Get values specific to this post state.
	if ps.Indexes.Data > len(tx.Data) {
		return nil, fmt.Errorf("txn data index %d out of bounds", ps.Indexes.Data)
	}
	if ps.Indexes.Value > len(tx.Value) {
		return nil, fmt.Errorf("txn value index %d out of bounds", ps.Indexes.Value)
	}
	if ps.Indexes.Gas > len(tx.GasLimit) {
		return nil, fmt.Errorf("txn gas limit index %d out of bounds", ps.Indexes.Gas)
	}
	dataHex := tx.Data[ps.Indexes.Data]
	valueHex := tx.Value[ps.Indexes.Value]
	gasLimit := tx.GasLimit[ps.Indexes.Gas]

	value := new(uint256.Int)
	if valueHex != "0x" {
		va, ok := math.ParseBig256(valueHex)
		if !ok {
			return nil, fmt.Errorf("invalid txn value %q", valueHex)
		}
		v, overflow := uint256.FromBig(va)
		if overflow {
			return nil, fmt.Errorf("invalid txn value (overflowed) %q", valueHex)
		}
		value = v
	}
	data, err := hex.DecodeString(strings.TrimPrefix(dataHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("invalid txn data %q", dataHex)
	}
	var accessList types.AccessList
	if tx.AccessLists != nil && tx.AccessLists[ps.Indexes.Data] != nil {
		accessList = *tx.AccessLists[ps.Indexes.Data]
	}

	var feeCap, tipCap big.Int

	// If baseFee provided, set gasPrice to effectiveGasPrice.
	gasPrice := tx.GasPrice
	if baseFee != nil {
		if tx.MaxFeePerGas == nil {
			tx.MaxFeePerGas = gasPrice
		}
		if tx.MaxFeePerGas == nil {
			tx.MaxFeePerGas = math.NewHexOrDecimal256(0)
		}
		if tx.MaxPriorityFeePerGas == nil {
			tx.MaxPriorityFeePerGas = tx.MaxFeePerGas
		}
		tipCap = big.Int(*tx.MaxPriorityFeePerGas)
		feeCap = big.Int(*tx.MaxFeePerGas)
		gp := math.BigMin(new(big.Int).Add(&tipCap, baseFee.ToBig()), &feeCap)
		gasPrice = math.NewHexOrDecimal256(gp.Int64())
	}
	if gasPrice == nil {
		return nil, errors.New("no gas price provided")
	}

	gpi := big.Int(*gasPrice)
	gasPriceInt := uint256.NewInt(gpi.Uint64())

	var blobFeeCap *big.Int
	if tx.BlobGasFeeCap != nil {
		blobFeeCap = (*big.Int)(tx.BlobGasFeeCap)
	}

	msg := types.NewMessage(
		from,
		to,
		uint64(tx.Nonce),
		value,
		uint64(gasLimit),
		gasPriceInt,
		uint256.MustFromBig(&feeCap),
		uint256.MustFromBig(&tipCap),
		data,
		accessList,
		true,  /* checkNonce */
		true,  /* checkTransaction */
		true,  /* checkGas */
		false, /* isFree */
		uint256.MustFromBig(blobFeeCap),
	)

	// Add authorizations when authorizationList was present in JSON.
	// An empty list [] still marks the tx as type-4 SetCode, affecting intrinsic gas.
	if tx.IsSetCodeTx {
		authorizations := make([]types.Authorization, len(tx.Authorizations))
		for i, auth := range tx.Authorizations {
			authorizations[i], err = auth.ToAuthorization()
			if err != nil {
				return nil, err
			}
		}
		msg.SetAuthorizations(authorizations)
	}

	// Add blob versioned hashes if present.
	if len(tx.BlobVersionedHashes) > 0 {
		msg.SetBlobVersionedHashes(tx.BlobVersionedHashes)
	}

	return msg, nil
}
