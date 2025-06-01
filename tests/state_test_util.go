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

package tests

import (
	context2 "context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/testutil"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// StateTest checks transaction processing without block context.
// See https://github.com/ethereum/EIPs/issues/176 for the test format specification.
type StateTest struct {
	json stJSON
}

// StateSubtest selects a specific configuration of a General State Test.
type StateSubtest struct {
	Fork  string
	Index int
}

func (t *StateTest) UnmarshalJSON(in []byte) error {
	return json.Unmarshal(in, &t.json)
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
	GasPrice             *math.HexOrDecimal256     `json:"gasPrice"`
	MaxFeePerGas         *math.HexOrDecimal256     `json:"maxFeePerGas"`
	MaxPriorityFeePerGas *math.HexOrDecimal256     `json:"maxPriorityFeePerGas"`
	Nonce                math.HexOrDecimal64       `json:"nonce"`
	GasLimit             []math.HexOrDecimal64     `json:"gasLimit"`
	PrivateKey           hexutil.Bytes             `json:"secretKey"`
	To                   string                    `json:"to"`
	Data                 []string                  `json:"data"`
	Value                []string                  `json:"value"`
	AccessLists          []*types.AccessList       `json:"accessLists,omitempty"`
	BlobGasFeeCap        *math.HexOrDecimal256     `json:"maxFeePerBlobGas,omitempty"`
	Authorizations       []types.JsonAuthorization `json:"authorizationList,omitempty"`
}

//go:generate gencodec -type stEnv -field-override stEnvMarshaling -out gen_stenv.go

type stEnv struct {
	Coinbase      common.Address `json:"currentCoinbase"   gencodec:"required"`
	Difficulty    *big.Int       `json:"currentDifficulty" gencodec:"required"`
	Random        *big.Int       `json:"currentRandom"     gencodec:"optional"`
	GasLimit      uint64         `json:"currentGasLimit"   gencodec:"required"`
	Number        uint64         `json:"currentNumber"     gencodec:"required"`
	Timestamp     uint64         `json:"currentTimestamp"  gencodec:"required"`
	BaseFee       *big.Int       `json:"currentBaseFee"    gencodec:"optional"`
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
	if baseConfig, ok = testutil.Forks[baseName]; !ok {
		return nil, nil, testutil.UnsupportedForkError{Name: baseName}
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
	var sub []StateSubtest
	for fork, pss := range t.json.Post {
		for i := range pss {
			sub = append(sub, StateSubtest{fork, i})
		}
	}
	return sub
}

// Run executes a specific subtest and verifies the post-state and logs
func (t *StateTest) Run(tx kv.TemporalRwTx, subtest StateSubtest, vmconfig vm.Config, dirs datadir.Dirs) (*state.IntraBlockState, common.Hash, error) {
	state, root, err := t.RunNoVerify(tx, subtest, vmconfig, dirs)
	if err != nil {
		return state, empty.RootHash, err
	}
	post := t.json.Post[subtest.Fork][subtest.Index]
	// N.B: We need to do this in a two-step process, because the first Commit takes care
	// of suicides, and we need to touch the coinbase _after_ it has potentially suicided.
	if root != common.Hash(post.Root) {
		return state, root, fmt.Errorf("post state root mismatch: got %x, want %x", root, post.Root)
	}
	if logs := rlpHash(state.Logs()); logs != common.Hash(post.Logs) {
		return state, root, fmt.Errorf("post state logs hash mismatch: got %x, want %x", logs, post.Logs)
	}
	return state, root, nil
}

// RunNoVerify runs a specific subtest and returns the statedb and post-state root
func (t *StateTest) RunNoVerify(tx kv.TemporalRwTx, subtest StateSubtest, vmconfig vm.Config, dirs datadir.Dirs) (*state.IntraBlockState, common.Hash, error) {
	config, eips, err := GetChainConfig(subtest.Fork)
	if err != nil {
		return nil, common.Hash{}, testutil.UnsupportedForkError{Name: subtest.Fork}
	}
	vmconfig.ExtraEips = eips
	block, _, err := core.GenesisToBlock(t.genesis(config), dirs, log.Root())
	if err != nil {
		return nil, common.Hash{}, testutil.UnsupportedForkError{Name: subtest.Fork}
	}

	readBlockNr := block.NumberU64()
	writeBlockNr := readBlockNr + 1

	_, err = MakePreState(&chain.Rules{}, tx, t.json.Pre, readBlockNr)
	if err != nil {
		return nil, common.Hash{}, testutil.UnsupportedForkError{Name: subtest.Fork}
	}

	txc := wrap.NewTxContainer(tx, nil)
	domains, err := state2.NewSharedDomains(txc.Ttx, log.New())
	if err != nil {
		return nil, common.Hash{}, testutil.UnsupportedForkError{Name: subtest.Fork}
	}
	defer domains.Close()
	blockNum, txNum := readBlockNr, uint64(1)

	r := rpchelper.NewLatestStateReader(txc.Ttx)
	w := rpchelper.NewLatestStateWriter(tx, domains, nil, writeBlockNr)
	statedb := state.New(r)

	var baseFee *big.Int
	if config.IsLondon(0) {
		baseFee = t.json.Env.BaseFee
		if baseFee == nil {
			// Retesteth uses `0x10` for genesis baseFee. Therefore, it defaults to
			// parent - 2 : 0xa as the basefee for 'this' context.
			baseFee = big.NewInt(0x0a)
		}
	}
	post := t.json.Post[subtest.Fork][subtest.Index]
	msg, err := toMessage(t.json.Tx, post, baseFee)
	if err != nil {
		return nil, common.Hash{}, err
	}
	if len(post.Tx) != 0 {
		txn, err := types.UnmarshalTransactionFromBinary(post.Tx, false /* blobTxnsAreWrappedWithBlobs */)
		if err != nil {
			return nil, common.Hash{}, err
		}
		msg, err = txn.AsMessage(*types.MakeSigner(config, 0, 0), baseFee, config.Rules(0, 0))
		if err != nil {
			return nil, common.Hash{}, err
		}
	}

	// Prepare the EVM.
	txContext := core.NewEVMTxContext(msg)
	header := block.HeaderNoCopy()
	//blockNum, txNum := header.Number.Uint64(), 1

	context := core.NewEVMBlockContext(header, core.GetHashFn(header, nil), nil, &t.json.Env.Coinbase, config)
	context.GetHash = vmTestBlockHash
	if baseFee != nil {
		context.BaseFee = new(uint256.Int)
		context.BaseFee.SetFromBig(baseFee)
	}
	if t.json.Env.Difficulty != nil {
		context.Difficulty = new(big.Int).Set(t.json.Env.Difficulty)
	}
	if config.IsLondon(0) && t.json.Env.Random != nil {
		rnd := common.BigToHash(t.json.Env.Random)
		context.PrevRanDao = &rnd
		context.Difficulty = big.NewInt(0)
	}
	if config.IsCancun(block.Time()) && t.json.Env.ExcessBlobGas != nil {
		context.BlobBaseFee, err = misc.GetBlobGasPrice(config, *t.json.Env.ExcessBlobGas, header.Time)
		if err != nil {
			return nil, common.Hash{}, err
		}
	}
	evm := vm.NewEVM(context, txContext, statedb, config, vmconfig)
	if vmconfig.Tracer != nil && vmconfig.Tracer.OnTxStart != nil {
		vmconfig.Tracer.OnTxStart(evm.GetVMContext(), nil, common.Address{})
	}

	// Execute the message.
	snapshot := statedb.Snapshot()
	gaspool := new(core.GasPool)
	gaspool.AddGas(block.GasLimit()).AddBlobGas(config.GetMaxBlobGasPerBlock(header.Time))
	res, err := core.ApplyMessage(evm, msg, gaspool, true /* refunds */, false /* gasBailout */, nil /* engine */)
	if err != nil {
		statedb.RevertToSnapshot(snapshot)
	}
	if vmconfig.Tracer != nil && vmconfig.Tracer.OnTxEnd != nil {
		vmconfig.Tracer.OnTxEnd(&types.Receipt{GasUsed: res.GasUsed}, nil)
	}

	if err = statedb.FinalizeTx(evm.ChainRules(), w); err != nil {
		return nil, common.Hash{}, err
	}
	if err = statedb.CommitBlock(evm.ChainRules(), w); err != nil {
		return nil, common.Hash{}, err
	}

	var root common.Hash
	rootBytes, err := domains.ComputeCommitment(context2.Background(), true, blockNum, txNum, "")
	if err != nil {
		return statedb, root, fmt.Errorf("ComputeCommitment: %w", err)
	}
	return statedb, common.BytesToHash(rootBytes), nil
}

func MakePreState(rules *chain.Rules, tx kv.TemporalRwTx, accounts types.GenesisAlloc, blockNr uint64) (*state.IntraBlockState, error) {
	r := rpchelper.NewLatestStateReader(tx)
	statedb := state.New(r)
	statedb.SetTxContext(0)
	for addr, a := range accounts {
		statedb.SetCode(addr, a.Code)
		statedb.SetNonce(addr, a.Nonce)
		var balance uint256.Int
		if a.Balance != nil {
			_ = balance.SetFromBig(a.Balance)
		}
		statedb.SetBalance(addr, balance, tracing.BalanceChangeUnspecified)
		for k, v := range a.Storage {
			key := k
			val := uint256.NewInt(0).SetBytes(v.Bytes())
			statedb.SetState(addr, key, *val)
		}

		if len(a.Code) > 0 || len(a.Storage) > 0 {
			statedb.SetIncarnation(addr, state.FirstContractIncarnation)

			var b [8]byte
			binary.BigEndian.PutUint64(b[:], state.FirstContractIncarnation)
			if err := tx.Put(kv.IncarnationMap, addr[:], b[:]); err != nil {
				return nil, err
			}
		}
	}

	domains, err := state2.NewSharedDomains(tx, log.New())
	if err != nil {
		return nil, err
	}
	defer domains.Close()

	w := rpchelper.NewLatestStateWriter(tx, domains, nil, blockNr-1)

	// Commit and re-open to start with a clean state.
	if err := statedb.FinalizeTx(rules, w); err != nil {
		return nil, err
	}
	if err := statedb.CommitBlock(rules, w); err != nil {
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
		Coinbase:   t.json.Env.Coinbase,
		Difficulty: t.json.Env.Difficulty,
		GasLimit:   t.json.Env.GasLimit,
		Number:     t.json.Env.Number,
		Timestamp:  t.json.Env.Timestamp,
		Alloc:      t.json.Pre,
	}
}

func rlpHash(x interface{}) (h common.Hash) {
	hw := sha3.NewLegacyKeccak256()
	if err := rlp.Encode(hw, x); err != nil {
		panic(err)
	}
	hw.Sum(h[:0])
	return h
}

func vmTestBlockHash(n uint64) (common.Hash, error) {
	return common.BytesToHash(crypto.Keccak256([]byte(new(big.Int).SetUint64(n).String()))), nil
}

func toMessage(tx stTransaction, ps stPostState, baseFee *big.Int) (core.Message, error) {
	// Derive sender from private key if present.
	var from common.Address
	if len(tx.PrivateKey) > 0 {
		key, err := crypto.ToECDSA(tx.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("invalid private key: %v", err)
		}
		from = crypto.PubkeyToAddress(key.PublicKey)
	}

	// Parse recipient if present.
	var to *common.Address
	if tx.To != "" {
		to = new(common.Address)
		if err := to.UnmarshalText([]byte(tx.To)); err != nil {
			return nil, fmt.Errorf("invalid to address: %v", err)
		}
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

		//feeCap = big.Int(*tx.MaxPriorityFeePerGas)
		//tipCap = big.Int(*tx.MaxFeePerGas)

		tipCap = big.Int(*tx.MaxPriorityFeePerGas)
		feeCap = big.Int(*tx.MaxFeePerGas)

		gp := math.BigMin(new(big.Int).Add(&tipCap, baseFee), &feeCap)
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

	// TODO the conversion to int64 then uint64 then new int isn't working!
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
		false, /* checkNonce */
		false, /* isFree */
		uint256.MustFromBig(blobFeeCap),
	)

	// Add authorizations if present.
	if len(tx.Authorizations) > 0 {
		authorizations := make([]types.Authorization, len(tx.Authorizations))
		for i, auth := range tx.Authorizations {
			authorizations[i], err = auth.ToAuthorization()
			if err != nil {
				return nil, err
			}
		}
		msg.SetAuthorizations(authorizations)
	}

	return msg, nil
}
