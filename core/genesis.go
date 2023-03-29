// Copyright 2014 The go-ethereum Authors
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

package core

import (
	"bytes"
	"context"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
)

//go:generate gencodec -type Genesis -field-override genesisSpecMarshaling -out gen_genesis.go
//go:generate gencodec -type GenesisAccount -field-override genesisAccountMarshaling -out gen_genesis_account.go

//go:embed allocs
var allocs embed.FS

var ErrGenesisNoConfig = errors.New("genesis has no chain configuration")

// Genesis specifies the header fields, state of a genesis block. It also defines hard
// fork switch-over blocks through the chain configuration.
type Genesis struct {
	Config     *chain.Config     `json:"config"`
	Nonce      uint64            `json:"nonce"`
	Timestamp  uint64            `json:"timestamp"`
	ExtraData  []byte            `json:"extraData"`
	GasLimit   uint64            `json:"gasLimit"   gencodec:"required"`
	Difficulty *big.Int          `json:"difficulty" gencodec:"required"`
	Mixhash    libcommon.Hash    `json:"mixHash"`
	Coinbase   libcommon.Address `json:"coinbase"`
	Alloc      GenesisAlloc      `json:"alloc"      gencodec:"required"`
	AuRaStep   uint64            `json:"auRaStep"`
	AuRaSeal   []byte            `json:"auRaSeal"`

	// These fields are used for consensus tests. Please don't use them
	// in actual genesis blocks.
	Number        uint64         `json:"number"`
	GasUsed       uint64         `json:"gasUsed"`
	ParentHash    libcommon.Hash `json:"parentHash"`
	BaseFee       *big.Int       `json:"baseFeePerGas"`
	ExcessDataGas *big.Int       `json:"excessDataGas"`
}

// GenesisAlloc specifies the initial state that is part of the genesis block.
type GenesisAlloc map[libcommon.Address]GenesisAccount

type AuthorityRoundSeal struct {
	/// Seal step.
	Step uint64 `json:"step"`
	/// Seal signature.
	Signature libcommon.Hash `json:"signature"`
}

var genesisTmpDB kv.RwDB
var genesisDBLock sync.Mutex

func (ga *GenesisAlloc) UnmarshalJSON(data []byte) error {
	m := make(map[common.UnprefixedAddress]GenesisAccount)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*ga = make(GenesisAlloc)
	for addr, a := range m {
		(*ga)[libcommon.Address(addr)] = a
	}
	return nil
}

// GenesisAccount is an account in the state of the genesis block.
// Either use "constructor" for deployment code or "code" directly for the final code.
type GenesisAccount struct {
	Constructor []byte                            `json:"constructor,omitempty"` // deployment code
	Code        []byte                            `json:"code,omitempty"`        // final contract code
	Storage     map[libcommon.Hash]libcommon.Hash `json:"storage,omitempty"`
	Balance     *big.Int                          `json:"balance" gencodec:"required"`
	Nonce       uint64                            `json:"nonce,omitempty"`
	PrivateKey  []byte                            `json:"secretKey,omitempty"` // for tests
}

// field type overrides for gencodec
type genesisSpecMarshaling struct {
	Nonce         math.HexOrDecimal64
	Timestamp     math.HexOrDecimal64
	ExtraData     hexutil.Bytes
	GasLimit      math.HexOrDecimal64
	GasUsed       math.HexOrDecimal64
	Number        math.HexOrDecimal64
	Difficulty    *math.HexOrDecimal256
	BaseFee       *math.HexOrDecimal256
	ExcessDataGas *math.HexOrDecimal256
	Alloc         map[common.UnprefixedAddress]GenesisAccount
}

type genesisAccountMarshaling struct {
	Constructor hexutil.Bytes
	Code        hexutil.Bytes
	Balance     *math.HexOrDecimal256
	Nonce       math.HexOrDecimal64
	Storage     map[storageJSON]storageJSON
	PrivateKey  hexutil.Bytes
}

// storageJSON represents a 256 bit byte array, but allows less than 256 bits when
// unmarshaling from hex.
type storageJSON libcommon.Hash

func (h *storageJSON) UnmarshalText(text []byte) error {
	text = bytes.TrimPrefix(text, []byte("0x"))
	if len(text) > 64 {
		return fmt.Errorf("too many hex characters in storage key/value %q", text)
	}
	offset := len(h) - len(text)/2 // pad on the left
	if _, err := hex.Decode(h[offset:], text); err != nil {
		return fmt.Errorf("invalid hex storage key/value %q", text)
	}
	return nil
}

func (h storageJSON) MarshalText() ([]byte, error) {
	return hexutil.Bytes(h[:]).MarshalText()
}

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New libcommon.Hash
}

func (e *GenesisMismatchError) Error() string {
	config := params.ChainConfigByGenesisHash(e.Stored)
	if config == nil {
		return fmt.Sprintf("database contains incompatible genesis (have %x, new %x)", e.Stored, e.New)
	}
	return fmt.Sprintf("database contains incompatible genesis (try with --chain=%s)", config.ChainName)
}
func (g *Genesis) configOrDefault(genesisHash libcommon.Hash) *chain.Config {
	if g != nil {
		return g.Config
	}

	config := params.ChainConfigByGenesisHash(genesisHash)
	if config != nil {
		return config
	} else {
		return params.AllProtocolChanges
	}
}

func sortedAllocKeys(m GenesisAlloc) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = string(k.Bytes())
		i++
	}
	slices.Sort(keys)
	return keys
}

// ToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func (g *Genesis) ToBlock(tmpDir string) (*types.Block, *state.IntraBlockState, error) {
	_ = g.Alloc //nil-check

	head := &types.Header{
		Number:        new(big.Int).SetUint64(g.Number),
		Nonce:         types.EncodeNonce(g.Nonce),
		Time:          g.Timestamp,
		ParentHash:    g.ParentHash,
		Extra:         g.ExtraData,
		GasLimit:      g.GasLimit,
		GasUsed:       g.GasUsed,
		Difficulty:    g.Difficulty,
		MixDigest:     g.Mixhash,
		Coinbase:      g.Coinbase,
		BaseFee:       g.BaseFee,
		ExcessDataGas: g.ExcessDataGas,
		AuRaStep:      g.AuRaStep,
		AuRaSeal:      g.AuRaSeal,
	}
	if g.GasLimit == 0 {
		head.GasLimit = params.GenesisGasLimit
	}
	if g.Difficulty == nil {
		head.Difficulty = params.GenesisDifficulty
	}
	if g.Config != nil && (g.Config.IsLondon(0)) {
		if g.BaseFee != nil {
			head.BaseFee = g.BaseFee
		} else {
			head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
		}
	}

	var withdrawals []*types.Withdrawal
	if g.Config != nil && (g.Config.IsShanghai(g.Timestamp)) {
		withdrawals = []*types.Withdrawal{}
	}

	var root libcommon.Hash
	var statedb *state.IntraBlockState
	wg := sync.WaitGroup{}
	wg.Add(1)
	var err error
	go func() { // we may run inside write tx, can't open 2nd write tx in same goroutine
		// TODO(yperbasis): use memdb.MemoryMutation instead
		defer wg.Done()
		genesisDBLock.Lock()
		defer genesisDBLock.Unlock()
		if genesisTmpDB == nil {
			genesisTmpDB = mdbx.NewMDBX(log.New()).InMem(tmpDir).MapSize(2 * datasize.GB).PageSize(2 * 4096).MustOpen()
		}
		var tx kv.RwTx
		if tx, err = genesisTmpDB.BeginRw(context.Background()); err != nil {
			return
		}
		defer tx.Rollback()
		r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)
		statedb = state.New(r)

		hasConstructorAllocation := false
		for _, account := range g.Alloc {
			if len(account.Constructor) > 0 {
				hasConstructorAllocation = true
				break
			}
		}
		// See https://github.com/NethermindEth/nethermind/blob/master/src/Nethermind/Nethermind.Consensus.AuRa/InitializationSteps/LoadGenesisBlockAuRa.cs
		if hasConstructorAllocation && g.Config.Aura != nil {
			statedb.CreateAccount(libcommon.Address{}, false)
		}

		keys := sortedAllocKeys(g.Alloc)
		for _, key := range keys {
			addr := libcommon.BytesToAddress([]byte(key))
			account := g.Alloc[addr]

			balance, overflow := uint256.FromBig(account.Balance)
			if overflow {
				panic("overflow at genesis allocs")
			}
			statedb.AddBalance(addr, balance)
			statedb.SetCode(addr, account.Code)
			statedb.SetNonce(addr, account.Nonce)
			for key, value := range account.Storage {
				key := key
				val := uint256.NewInt(0).SetBytes(value.Bytes())
				statedb.SetState(addr, &key, *val)
			}

			if len(account.Constructor) > 0 {
				if _, err = SysCreate(addr, account.Constructor, *g.Config, statedb, head); err != nil {
					return
				}
			}

			if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
				statedb.SetIncarnation(addr, state.FirstContractIncarnation)
			}
		}
		if err = statedb.FinalizeTx(&chain.Rules{}, w); err != nil {
			return
		}
		if root, err = trie.CalcRoot("genesis", tx); err != nil {
			return
		}
	}()
	wg.Wait()
	if err != nil {
		return nil, nil, err
	}

	head.Root = root

	return types.NewBlock(head, nil, nil, nil, withdrawals), statedb, nil
}

// MustCommitGenesis writes the genesis block and state to db, panicking on error.
// The block is committed as the canonical head block.
func (g *Genesis) MustCommit(db kv.RwDB, tmpDir string) *types.Block {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	block, _, err := write(tx, g, tmpDir)
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	return block
}
