// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package tests

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/holiman/uint256"
	"golang.org/x/crypto/sha3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/fixedgas"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/trie"
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
	Out  hexutility.Bytes         `json:"out"`
	Post map[string][]stPostState `json:"post"`
}

type stPostState struct {
	Root            common.UnprefixedHash `json:"hash"`
	Logs            common.UnprefixedHash `json:"logs"`
	Tx              hexutility.Bytes      `json:"txbytes"`
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
	PrivateKey           hexutility.Bytes      `json:"secretKey"`
	To                   string                `json:"to"`
	Data                 []string              `json:"data"`
	Value                []string              `json:"value"`
	AccessLists          []*types2.AccessList  `json:"accessLists,omitempty"`
	BlobGasFeeCap        *math.HexOrDecimal256 `json:"maxFeePerBlobGas,omitempty"`
}

//go:generate gencodec -type stEnv -field-override stEnvMarshaling -out gen_stenv.go

type stEnv struct {
	Coinbase   libcommon.Address `json:"currentCoinbase"   gencodec:"required"`
	Difficulty *big.Int          `json:"currentDifficulty" gencodec:"required"`
	Random     *big.Int          `json:"currentRandom"     gencodec:"optional"`
	GasLimit   uint64            `json:"currentGasLimit"   gencodec:"required"`
	Number     uint64            `json:"currentNumber"     gencodec:"required"`
	Timestamp  uint64            `json:"currentTimestamp"  gencodec:"required"`
	BaseFee    *big.Int          `json:"currentBaseFee"    gencodec:"optional"`
}

type stEnvMarshaling struct {
	Coinbase   common.UnprefixedAddress
	Difficulty *math.HexOrDecimal256
	Random     *math.HexOrDecimal256
	GasLimit   math.HexOrDecimal64
	Number     math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
	BaseFee    *math.HexOrDecimal256
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
	if baseConfig, ok = Forks[baseName]; !ok {
		return nil, nil, UnsupportedForkError{baseName}
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
func (t *StateTest) Run(tx kv.RwTx, subtest StateSubtest, vmconfig vm.Config) (*state.IntraBlockState, error) {
	state, root, err := t.RunNoVerify(tx, subtest, vmconfig)
	if err != nil {
		return state, err
	}
	post := t.json.Post[subtest.Fork][subtest.Index]
	// N.B: We need to do this in a two-step process, because the first Commit takes care
	// of suicides, and we need to touch the coinbase _after_ it has potentially suicided.
	if root != libcommon.Hash(post.Root) {
		return state, fmt.Errorf("post state root mismatch: got %x, want %x", root, post.Root)
	}
	if logs := rlpHash(state.Logs()); logs != libcommon.Hash(post.Logs) {
		return state, fmt.Errorf("post state logs hash mismatch: got %x, want %x", logs, post.Logs)
	}
	return state, nil
}

// RunNoVerify runs a specific subtest and returns the statedb and post-state root
func (t *StateTest) RunNoVerify(tx kv.RwTx, subtest StateSubtest, vmconfig vm.Config) (*state.IntraBlockState, libcommon.Hash, error) {
	config, eips, err := GetChainConfig(subtest.Fork)
	if err != nil {
		return nil, libcommon.Hash{}, UnsupportedForkError{subtest.Fork}
	}
	vmconfig.ExtraEips = eips
	block, _, err := core.GenesisToBlock(t.genesis(config), "")
	if err != nil {
		return nil, libcommon.Hash{}, UnsupportedForkError{subtest.Fork}
	}

	readBlockNr := block.NumberU64()
	writeBlockNr := readBlockNr + 1

	_, err = MakePreState(&chain.Rules{}, tx, t.json.Pre, readBlockNr)
	if err != nil {
		return nil, libcommon.Hash{}, UnsupportedForkError{subtest.Fork}
	}

	r := rpchelper.NewLatestStateReader(tx)
	statedb := state.New(r)

	var w state.StateWriter
	if ethconfig.EnableHistoryV4InTest {
		panic("implement me")
	} else {
		w = state.NewPlainStateWriter(tx, nil, writeBlockNr)
	}

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
		return nil, libcommon.Hash{}, err
	}
	if len(post.Tx) != 0 {
		txn, err := types.UnmarshalTransactionFromBinary(post.Tx)
		if err != nil {
			return nil, libcommon.Hash{}, err
		}
		msg, err = txn.AsMessage(*types.MakeSigner(config, 0, 0), baseFee, config.Rules(0, 0))
		if err != nil {
			return nil, libcommon.Hash{}, err
		}
	}

	// Prepare the EVM.
	txContext := core.NewEVMTxContext(msg)
	header := block.Header()
	context := core.NewEVMBlockContext(header, core.GetHashFn(header, nil), nil, &t.json.Env.Coinbase)
	context.GetHash = vmTestBlockHash
	if baseFee != nil {
		context.BaseFee = new(uint256.Int)
		context.BaseFee.SetFromBig(baseFee)
	}
	if t.json.Env.Random != nil {
		rnd := libcommon.BigToHash(t.json.Env.Random)
		context.PrevRanDao = &rnd
	}
	evm := vm.NewEVM(context, txContext, statedb, config, vmconfig)

	// Execute the message.
	snapshot := statedb.Snapshot()
	gaspool := new(core.GasPool)
	gaspool.AddGas(block.GasLimit()).AddBlobGas(fixedgas.MaxBlobGasPerBlock)
	if _, err = core.ApplyMessage(evm, msg, gaspool, true /* refunds */, false /* gasBailout */); err != nil {
		statedb.RevertToSnapshot(snapshot)
	}

	if err = statedb.FinalizeTx(evm.ChainRules(), w); err != nil {
		return nil, libcommon.Hash{}, err
	}
	if err = statedb.CommitBlock(evm.ChainRules(), w); err != nil {
		return nil, libcommon.Hash{}, err
	}
	// Generate hashed state
	c, err := tx.RwCursor(kv.PlainState)
	if err != nil {
		return nil, libcommon.Hash{}, err
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return nil, libcommon.Hash{}, fmt.Errorf("interate over plain state: %w", err)
		}
		var newK []byte
		if len(k) == length.Addr {
			newK = make([]byte, length.Hash)
		} else {
			newK = make([]byte, length.Hash*2+length.Incarnation)
		}
		h.Sha.Reset()
		//nolint:errcheck
		h.Sha.Write(k[:length.Addr])
		//nolint:errcheck
		h.Sha.Read(newK[:length.Hash])
		if len(k) > length.Addr {
			copy(newK[length.Hash:], k[length.Addr:length.Addr+length.Incarnation])
			h.Sha.Reset()
			//nolint:errcheck
			h.Sha.Write(k[length.Addr+length.Incarnation:])
			//nolint:errcheck
			h.Sha.Read(newK[length.Hash+length.Incarnation:])
			if err = tx.Put(kv.HashedStorage, newK, common.CopyBytes(v)); err != nil {
				return nil, libcommon.Hash{}, fmt.Errorf("insert hashed key: %w", err)
			}
		} else {
			if err = tx.Put(kv.HashedAccounts, newK, common.CopyBytes(v)); err != nil {
				return nil, libcommon.Hash{}, fmt.Errorf("insert hashed key: %w", err)
			}
		}
	}
	c.Close()

	root, err := trie.CalcRoot("", tx)
	if err != nil {
		return nil, libcommon.Hash{}, fmt.Errorf("error calculating state root: %w", err)
	}
	return statedb, root, nil
}

func MakePreState(rules *chain.Rules, tx kv.RwTx, accounts types.GenesisAlloc, blockNr uint64) (*state.IntraBlockState, error) {
	r := rpchelper.NewLatestStateReader(tx)
	statedb := state.New(r)
	for addr, a := range accounts {
		statedb.SetCode(addr, a.Code)
		statedb.SetNonce(addr, a.Nonce)
		balance := uint256.NewInt(0)
		if a.Balance != nil {
			balance, _ = uint256.FromBig(a.Balance)
		}
		statedb.SetBalance(addr, balance)
		for k, v := range a.Storage {
			key := k
			val := uint256.NewInt(0).SetBytes(v.Bytes())
			statedb.SetState(addr, &key, *val)
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

	var w state.StateWriter
	if ethconfig.EnableHistoryV4InTest {
		panic("implement me")
	} else {
		w = state.NewPlainStateWriter(tx, nil, blockNr+1)
	}
	// Commit and re-open to start with a clean state.
	if err := statedb.FinalizeTx(rules, w); err != nil {
		return nil, err
	}
	if err := statedb.CommitBlock(rules, w); err != nil {
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

func rlpHash(x interface{}) (h libcommon.Hash) {
	hw := sha3.NewLegacyKeccak256()
	if err := rlp.Encode(hw, x); err != nil {
		panic(err)
	}
	hw.Sum(h[:0])
	return h
}

func vmTestBlockHash(n uint64) libcommon.Hash {
	return libcommon.BytesToHash(crypto.Keccak256([]byte(big.NewInt(int64(n)).String())))
}

func toMessage(tx stTransaction, ps stPostState, baseFee *big.Int) (core.Message, error) {
	// Derive sender from private key if present.
	var from libcommon.Address
	if len(tx.PrivateKey) > 0 {
		key, err := crypto.ToECDSA(tx.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("invalid private key: %v", err)
		}
		from = crypto.PubkeyToAddress(key.PublicKey)
	}

	// Parse recipient if present.
	var to *libcommon.Address
	if tx.To != "" {
		to = new(libcommon.Address)
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
	var accessList types2.AccessList
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

		feeCap = big.Int(*tx.MaxPriorityFeePerGas)
		tipCap = big.Int(*tx.MaxFeePerGas)

		gp := math.BigMin(new(big.Int).Add(&feeCap, baseFee), &tipCap)
		gasPrice = math.NewHexOrDecimal256(gp.Int64())
	}
	if gasPrice == nil {
		return nil, fmt.Errorf("no gas price provided")
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

	return msg, nil
}
