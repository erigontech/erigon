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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

//go:generate gencodec -type Genesis -field-override genesisSpecMarshaling -out gen_genesis.go
//go:generate gencodec -type GenesisAccount -field-override genesisAccountMarshaling -out gen_genesis_account.go

//go:embed allocs
var allocs embed.FS

var ErrGenesisNoConfig = errors.New("genesis has no chain configuration")

// Genesis specifies the header fields, state of a genesis block. It also defines hard
// fork switch-over blocks through the chain configuration.
type Genesis struct {
	Config     *params.ChainConfig `json:"config"`
	Nonce      uint64              `json:"nonce"`
	Timestamp  uint64              `json:"timestamp"`
	ExtraData  []byte              `json:"extraData"`
	GasLimit   uint64              `json:"gasLimit"   gencodec:"required"`
	Difficulty *big.Int            `json:"difficulty" gencodec:"required"`
	Mixhash    common.Hash         `json:"mixHash"`
	Coinbase   common.Address      `json:"coinbase"`
	Alloc      GenesisAlloc        `json:"alloc"      gencodec:"required"`
	AuRaStep   uint64              `json:"auRaStep"`
	AuRaSeal   []byte              `json:"auRaSeal"`

	// These fields are used for consensus tests. Please don't use them
	// in actual genesis blocks.
	Number     uint64      `json:"number"`
	GasUsed    uint64      `json:"gasUsed"`
	ParentHash common.Hash `json:"parentHash"`
	BaseFee    *big.Int    `json:"baseFeePerGas"`
}

// GenesisAlloc specifies the initial state that is part of the genesis block.
type GenesisAlloc map[common.Address]GenesisAccount

type AuthorityRoundSeal struct {
	// / Seal step.
	Step uint64 `json:"step"`
	// / Seal signature.
	Signature common.Hash `json:"signature"`
}

func (ga *GenesisAlloc) UnmarshalJSON(data []byte) error {
	m := make(map[common.UnprefixedAddress]GenesisAccount)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*ga = make(GenesisAlloc)
	for addr, a := range m {
		(*ga)[common.Address(addr)] = a
	}
	return nil
}

// GenesisAccount is an account in the state of the genesis block.
// Either use "constructor" for deployment code or "code" directly for the final code.
type GenesisAccount struct {
	Constructor []byte                      `json:"constructor,omitempty"` // deployment code
	Code        []byte                      `json:"code,omitempty"`        // final contract code
	Storage     map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance     *big.Int                    `json:"balance" gencodec:"required"`
	Nonce       uint64                      `json:"nonce,omitempty"`
	PrivateKey  []byte                      `json:"secretKey,omitempty"` // for tests
}

// field type overrides for gencodec
type genesisSpecMarshaling struct {
	Nonce      math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
	ExtraData  hexutil.Bytes
	GasLimit   math.HexOrDecimal64
	GasUsed    math.HexOrDecimal64
	Number     math.HexOrDecimal64
	Difficulty *math.HexOrDecimal256
	BaseFee    *math.HexOrDecimal256
	Alloc      map[common.UnprefixedAddress]GenesisAccount
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
type storageJSON common.Hash

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
	Stored, New common.Hash
}

func (e *GenesisMismatchError) Error() string {
	config := params.ChainConfigByGenesisHash(e.Stored)
	if config == nil {
		return fmt.Sprintf("database contains incompatible genesis (have %x, new %x)", e.Stored, e.New)
	}
	return fmt.Sprintf("database contains incompatible genesis (try with --chain=%s)", config.ChainName)
}

// CommitGenesisBlock writes or updates the genesis block in db.
// The block that will be used is:
//
//	                     genesis == nil       genesis != nil
//	                  +------------------------------------------
//	db has no genesis |  main-net default  |  genesis
//	db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration is never nil.
func CommitGenesisBlock(db kv.RwDB, genesis *Genesis) (*params.ChainConfig, *types.Block, error) {
	return CommitGenesisBlockWithOverride(db, genesis, nil, nil)
}

func CommitGenesisBlockWithOverride(db kv.RwDB, genesis *Genesis, overrideMergeNetsplitBlock, overrideTerminalTotalDifficulty *big.Int) (*params.ChainConfig, *types.Block, error) {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	c, b, err := WriteGenesisBlock(tx, genesis, overrideMergeNetsplitBlock, overrideTerminalTotalDifficulty)
	if err != nil {
		return c, b, err
	}
	err = tx.Commit()
	if err != nil {
		return c, b, err
	}
	return c, b, nil
}

func MustCommitGenesisBlock(db kv.RwDB, genesis *Genesis) (*params.ChainConfig, *types.Block) {
	c, b, err := CommitGenesisBlock(db, genesis)
	if err != nil {
		panic(err)
	}
	return c, b
}

func WriteGenesisBlock(db kv.RwTx, genesis *Genesis, overrideMergeNetsplitBlock, overrideTerminalTotalDifficulty *big.Int) (*params.ChainConfig, *types.Block, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllProtocolChanges, nil, ErrGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	storedHash, storedErr := rawdb.ReadCanonicalHash(db, 0)
	if storedErr != nil {
		return nil, nil, storedErr
	}

	applyOverrides := func(config *params.ChainConfig) {
		if overrideMergeNetsplitBlock != nil {
			config.MergeNetsplitBlock = overrideMergeNetsplitBlock
		}
		if overrideTerminalTotalDifficulty != nil {
			config.TerminalTotalDifficulty = overrideTerminalTotalDifficulty
		}
	}

	if (storedHash == common.Hash{}) {
		custom := true
		if genesis == nil {
			log.Info("Writing default main-net genesis block")
			genesis = DefaultGenesisBlock()
			custom = false
		}
		applyOverrides(genesis.Config)
		block, _, err1 := genesis.Write(db)
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		if custom {
			log.Info("Writing custom genesis block", "hash", block.Hash().String())
		}
		return genesis.Config, block, nil
	}

	// Check whether the genesis block is already written.
	if genesis != nil {
		block, _, err1 := genesis.ToBlock()
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		hash := block.Hash()
		if hash != storedHash {
			return genesis.Config, block, &GenesisMismatchError{storedHash, hash}
		}
	}
	storedBlock, err := rawdb.ReadBlockByHash(db, storedHash)
	if err != nil {
		return genesis.Config, nil, err
	}
	// Get the existing chain configuration.
	newCfg := genesis.configOrDefault(storedHash)
	applyOverrides(newCfg)
	if err := newCfg.CheckConfigForkOrder(); err != nil {
		return newCfg, nil, err
	}
	storedCfg, storedErr := rawdb.ReadChainConfig(db, storedHash)
	if storedErr != nil {
		return newCfg, nil, storedErr
	}
	if storedCfg == nil {
		log.Warn("Found genesis block without chain config")
		err1 := rawdb.WriteChainConfig(db, storedHash, newCfg)
		if err1 != nil {
			return newCfg, nil, err1
		}
		return newCfg, storedBlock, nil
	}
	// Special case: don't change the existing config of a private chain if no new
	// config is supplied. This is useful, for example, to preserve DB config created by erigon init.
	// In that case, only apply the overrides.
	if genesis == nil && params.ChainConfigByGenesisHash(storedHash) == nil {
		newCfg = storedCfg
		applyOverrides(newCfg)
	}
	// Check config compatibility and write the config. Compatibility errors
	// are returned to the caller unless we're already at block zero.
	height := rawdb.ReadHeaderNumber(db, rawdb.ReadHeadHeaderHash(db))
	if height != nil {
		compatibilityErr := storedCfg.CheckCompatible(newCfg, *height)
		if compatibilityErr != nil && *height != 0 && compatibilityErr.RewindTo != 0 {
			return newCfg, storedBlock, compatibilityErr
		}
	}
	if err := rawdb.WriteChainConfig(db, storedHash, newCfg); err != nil {
		return newCfg, nil, err
	}
	return newCfg, storedBlock, nil
}

func (g *Genesis) configOrDefault(genesisHash common.Hash) *params.ChainConfig {
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
func (g *Genesis) ToBlock() (*types.Block, *state.IntraBlockState, error) {
	_ = g.Alloc // nil-check

	head := &types.Header{
		Number:     new(big.Int).SetUint64(g.Number),
		Nonce:      types.EncodeNonce(g.Nonce),
		Time:       g.Timestamp,
		ParentHash: g.ParentHash,
		Extra:      g.ExtraData,
		GasLimit:   g.GasLimit,
		GasUsed:    g.GasUsed,
		Difficulty: g.Difficulty,
		MixDigest:  g.Mixhash,
		Coinbase:   g.Coinbase,
		BaseFee:    g.BaseFee,
		AuRaStep:   g.AuRaStep,
		AuRaSeal:   g.AuRaSeal,
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

	var root common.Hash
	var statedb *state.IntraBlockState
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() { // we may run inside write tx, can't open 2nd write tx in same goroutine
		// TODO(yperbasis): use memdb.MemoryMutation instead
		defer wg.Done()
		tmpDB := mdbx.NewMDBX(log.New()).InMem("").MapSize(2 * datasize.GB).MustOpen()
		defer tmpDB.Close()
		tx, err := tmpDB.BeginRw(context.Background())
		if err != nil {
			panic(err)
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
			statedb.CreateAccount(common.Address{}, false)
		}

		keys := sortedAllocKeys(g.Alloc)
		for _, key := range keys {
			addr := common.BytesToAddress([]byte(key))
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
				_, err := SysCreate(addr, account.Constructor, *g.Config, statedb, head)
				if err != nil {
					panic(err)
				}
			}

			if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
				statedb.SetIncarnation(addr, state.FirstContractIncarnation)
			}
		}
		if err := statedb.FinalizeTx(&params.Rules{}, w); err != nil {
			panic(err)
		}
		root, err = trie.CalcRoot("genesis", tx)
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	head.Root = root

	return types.NewBlock(head, nil, nil, nil, nil), statedb, nil
}

func (g *Genesis) WriteGenesisState(tx kv.RwTx) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err := g.ToBlock()
	if err != nil {
		return nil, nil, err
	}
	for addr, account := range g.Alloc {
		if len(account.Code) > 0 || len(account.Storage) > 0 {
			// Special case for weird tests - inaccessible storage
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], state.FirstContractIncarnation)
			if err := tx.Put(kv.IncarnationMap, addr[:], b[:]); err != nil {
				return nil, nil, err
			}
		}
	}

	if block.Number().Sign() != 0 {
		return nil, statedb, fmt.Errorf("can't commit genesis block with number > 0")
	}

	blockWriter := state.NewPlainStateWriter(tx, tx, 0)

	if err := statedb.CommitBlock(&params.Rules{}, blockWriter); err != nil {
		return nil, statedb, fmt.Errorf("cannot write state: %w", err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		return nil, statedb, fmt.Errorf("cannot write change sets: %w", err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		return nil, statedb, fmt.Errorf("cannot write history: %w", err)
	}
	return block, statedb, nil
}

func (g *Genesis) MustWrite(tx kv.RwTx, history bool) (*types.Block, *state.IntraBlockState) {
	b, s, err := g.Write(tx)
	if err != nil {
		panic(err)
	}
	return b, s
}

// Write writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func (g *Genesis) Write(tx kv.RwTx) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err2 := g.WriteGenesisState(tx)
	if err2 != nil {
		return block, statedb, err2
	}
	config := g.Config
	if config == nil {
		config = params.AllProtocolChanges
	}
	if err := config.CheckConfigForkOrder(); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteTd(tx, block.Hash(), block.NumberU64(), g.Difficulty); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteBlock(tx, block); err != nil {
		return nil, nil, err
	}
	if err := rawdb.TxNums.WriteForGenesis(tx, 1); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteReceipts(tx, block.NumberU64(), nil); err != nil {
		return nil, nil, err
	}

	if err := rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64()); err != nil {
		return nil, nil, err
	}

	rawdb.WriteHeadBlockHash(tx, block.Hash())
	if err := rawdb.WriteHeadHeaderHash(tx, block.Hash()); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteChainConfig(tx, block.Hash(), config); err != nil {
		return nil, nil, err
	}
	// We support ethash/serenity for issuance (for now)
	if g.Config.Consensus != params.EtHashConsensus {
		return block, statedb, nil
	}
	// Issuance is the sum of allocs
	genesisIssuance := big.NewInt(0)
	for _, account := range g.Alloc {
		genesisIssuance.Add(genesisIssuance, account.Balance)
	}

	// BlockReward can be present at genesis
	if block.Header().Difficulty.Cmp(serenity.SerenityDifficulty) == 0 {
		// Proof-of-stake is 0.3 ether per block (TODO: revisit)
		genesisIssuance.Add(genesisIssuance, serenity.RewardSerenity)
	} else {
		blockReward, _ := ethash.AccumulateRewards(g.Config, block.Header(), nil)
		// Set BlockReward
		genesisIssuance.Add(genesisIssuance, blockReward.ToBig())
	}
	if err := rawdb.WriteTotalIssued(tx, 0, genesisIssuance); err != nil {
		return nil, nil, err
	}
	return block, statedb, rawdb.WriteTotalBurnt(tx, 0, common.Big0)
}

// MustCommit writes the genesis block and state to db, panicking on error.
// The block is committed as the canonical head block.
func (g *Genesis) MustCommit(db kv.RwDB) *types.Block {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	block, _ := g.MustWrite(tx, true)
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	return block
}

// GenesisBlockForTesting creates and writes a block in which addr has the given wei balance.
func GenesisBlockForTesting(db kv.RwDB, addr common.Address, balance *big.Int) *types.Block {
	g := Genesis{Alloc: GenesisAlloc{addr: {Balance: balance}}, Config: params.TestChainConfig}
	block := g.MustCommit(db)
	return block
}

type GenAccount struct {
	Addr    common.Address
	Balance *big.Int
}

func GenesisWithAccounts(db kv.RwDB, accs []GenAccount) *types.Block {
	g := Genesis{Config: params.TestChainConfig}
	allocs := make(map[common.Address]GenesisAccount)
	for _, acc := range accs {
		allocs[acc.Addr] = GenesisAccount{Balance: acc.Balance}
	}
	g.Alloc = allocs
	block := g.MustCommit(db)
	return block
}

// DefaultGenesisBlock returns the Ethereum main net genesis block.
func DefaultGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.MainnetChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa"),
		GasLimit:   5000,
		Difficulty: big.NewInt(17179869184),
		Alloc:      readPrealloc("allocs/mainnet.json"),
	}
}

// DefaultSepoliaGenesisBlock returns the Sepolia network genesis block.
func DefaultSepoliaGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.SepoliaChainConfig,
		Nonce:      0,
		ExtraData:  []byte("Sepolia, Athens, Attica, Greece!"),
		GasLimit:   30000000,
		Difficulty: big.NewInt(131072),
		Timestamp:  1633267481,
		Alloc:      readPrealloc("allocs/sepolia.json"),
	}
}

// DefaultRopstenGenesisBlock returns the Ropsten network genesis block.
func DefaultRopstenGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.RopstenChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x3535353535353535353535353535353535353535353535353535353535353535"),
		GasLimit:   16777216,
		Difficulty: big.NewInt(1048576),
		Alloc:      readPrealloc("allocs/ropsten.json"),
	}
}

// DefaultRinkebyGenesisBlock returns the Rinkeby network genesis block.
func DefaultRinkebyGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.RinkebyChainConfig,
		Timestamp:  1492009146,
		ExtraData:  hexutil.MustDecode("0x52657370656374206d7920617574686f7269746168207e452e436172746d616e42eb768f2244c8811c63729a21a3569731535f067ffc57839b00206d1ad20c69a1981b489f772031b279182d99e65703f0076e4812653aab85fca0f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   4700000,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/rinkeby.json"),
	}
}

// DefaultGoerliGenesisBlock returns the Görli network genesis block.
func DefaultGoerliGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.GoerliChainConfig,
		Timestamp:  1548854791,
		ExtraData:  hexutil.MustDecode("0x22466c6578692069732061207468696e6722202d204166726900000000000000e0a2bd4258d2768837baa26a28fe71dc079f84c70000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   10485760,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/goerli.json"),
	}
}

// DefaultShandongGenesisBlock returns the Shandong network genesis block.
// Source: https://github.com/hyperledger/besu/blob/main/config/src/main/resources/shandong.json
func DefaultShandongGenesisBlock() *Genesis {
	return &Genesis{
		Config:    params.ShandongChainConfig,
		Timestamp: 1667641735,
		// ExtraData:  nil,
		GasLimit:   0x400000,
		BaseFee:    big.NewInt(1000000000),
		Difficulty: big.NewInt(1),
		Nonce:      0x1234,
		Alloc:      readPrealloc("allocs/shandong.json"),
		// Coinbase: 0x0
		// Mixhash: 0x0
		// ParentHash: 0x0
	}
}

func DefaultSokolGenesisBlock() *Genesis {
	/*
		header rlp: f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
	*/
	return &Genesis{
		Config:     params.SokolChainConfig,
		Timestamp:  0x0,
		AuRaSeal:   common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x663BE0,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/sokol.json"),
	}
}

func DefaultBSCGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.BSCChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000002a7cdd959bfe8d9487b2a43b33565295a698f7e26488aa4d1955ee33403f8ccb1d4de5fb97c7ade29ef9f4360c606c7ab4db26b016007d3ad0ab86a0ee01c3b1283aa067c58eab4709f85e99d46de5fe685b1ded8013785d6623cc18d214320b6bb6475978f3adfc719c99674c072166708589033e2d9afec2be4ec20253b8642161bc3f444f53679c1f3d472f7be8361c80a4c1e7e9aaf001d0877f1cfde218ce2fd7544e0b2cc94692d4a704debef7bcb61328b8f7166496996a7da21cf1f1b04d9b3e26a3d0772d4c407bbe49438ed859fe965b140dcf1aab71a96bbad7cf34b5fa511d8e963dbba288b1960e75d64430b3230294d12c6ab2aac5c2cd68e80b16b581ea0a6e3c511bbd10f4519ece37dc24887e11b55d7ae2f5b9e386cd1b50a4550696d957cb4900f03a82012708dafc9e1b880fd083b32182b869be8e0922b81f8e175ffde54d797fe11eb03f9e3bf75f1d68bf0b8b6fb4e317a0f9d6f03eaf8ce6675bc60d8c4d90829ce8f72d0163c1d5cf348a862d55063035e7a025f4da968de7e4d7e4004197917f4070f1d6caa02bbebaebb5d7e581e4b66559e635f805ff0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      readPrealloc("allocs/bsc.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

func DefaultChapelGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.ChapelChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001284214b9b9c85549ab3d2b972df0deef66ac2c9b71b214cb885500844365e95cd9942c7276e7fd8a2959d3f95eae5dc7d70144ce1b73b403b7eb6e0980a75ecd1309ea12fa2ed87a8744fbfc9b863d535552c16704d214347f29fa77f77da6d75d7c752f474cf03cceff28abc65c9cbae594f725c80e12d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      readPrealloc("allocs/chapel.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

func DefaultRialtoGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.RialtoChainConfig,
		Nonce:      0x00,
		Timestamp:  0x5e9da7ce,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001284214b9b9c85549ab3d2b972df0deef66ac2c9b71b214cb885500844365e95cd9942c7276e7fd8a2959d3f95eae5dc7d70144ce1b73b403b7eb6e0980a75ecd1309ea12fa2ed87a8744fbfc9b863d535552c16704d214347f29fa77f77da6d75d7c752f474cf03cceff28abc65c9cbae594f725c80e12d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x2625a00,
		Difficulty: big.NewInt(0x1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0xffffFFFfFFffffffffffffffFfFFFfffFFFfFFfE"),
		Alloc:      readPrealloc("allocs/bsc.json"),
		Number:     0x00,
		GasUsed:    0x00,
	}
}

func DefaultFermionGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.FermionChainConfig,
		Timestamp:  0x0,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000003a03f6d88437328ce8623ef5e80c67383704ebc13ec60da1858ec7fa8edd0dc736611dba9ab4399942d5d120ad9c1692c5fa72dca20657254bbaa08d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x5B8D80,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/fermion.json"),
	}
}

func DefaultMumbaiGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.MumbaiChainConfig,
		Nonce:      0,
		Timestamp:  1558348305,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/mumbai.json"),
	}
}

// DefaultBorMainnet returns the Bor Mainnet network gensis block.
func DefaultBorMainnetGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.BorMainnetChainConfig,
		Nonce:      0,
		Timestamp:  1590824836,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_mainnet.json"),
	}
}

func DefaultBorDevnetGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.BorDevnetChainConfig,
		Nonce:      0,
		Timestamp:  1558348305,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_devnet.json"),
	}
}

func DefaultGnosisGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.GnosisChainConfig,
		Timestamp:  0,
		AuRaSeal:   common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/gnosis.json"),
	}
}

func DefaultChiadoGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.ChiadoChainConfig,
		Timestamp:  0,
		AuRaSeal:   common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/chiado.json"),
	}
}

// Pre-calculated version of:
//
//	DevnetSignPrivateKey = crypto.HexToECDSA(sha256.Sum256([]byte("erigon devnet key")))
//	DevnetEtherbase=crypto.PubkeyToAddress(DevnetSignPrivateKey.PublicKey)
var DevnetSignPrivateKey, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")
var DevnetEtherbase = common.HexToAddress("67b1d87101671b127f5f8714789c7192f7ad340e")

// DeveloperGenesisBlock returns the 'geth --dev' genesis block.
func DeveloperGenesisBlock(period uint64, faucet common.Address) *Genesis {
	// Override the default period to the user requested one
	config := *params.AllCliqueProtocolChanges
	config.Clique.Period = period

	// Assemble and return the genesis with the precompiles and faucet pre-funded
	return &Genesis{
		Config:     &config,
		ExtraData:  append(append(make([]byte, 32), faucet[:]...), make([]byte, crypto.SignatureLength)...),
		GasLimit:   11500000,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/dev.json"),
	}
}

func readPrealloc(filename string) GenesisAlloc {
	f, err := allocs.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Could not open genesis preallocation for %s: %v", filename, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	ga := make(GenesisAlloc)
	err = decoder.Decode(&ga)
	if err != nil {
		panic(fmt.Sprintf("Could not parse genesis preallocation for %s: %v", filename, err))
	}
	return ga
}

func DefaultGenesisBlockByChainName(chain string) *Genesis {
	switch chain {
	case networkname.MainnetChainName:
		return DefaultGenesisBlock()
	case networkname.SepoliaChainName:
		return DefaultSepoliaGenesisBlock()
	case networkname.RopstenChainName:
		return DefaultRopstenGenesisBlock()
	case networkname.ShandongChainName:
		return DefaultShandongGenesisBlock()
	case networkname.RinkebyChainName:
		return DefaultRinkebyGenesisBlock()
	case networkname.GoerliChainName:
		return DefaultGoerliGenesisBlock()
	case networkname.SokolChainName:
		return DefaultSokolGenesisBlock()
	case networkname.FermionChainName:
		return DefaultFermionGenesisBlock()
	case networkname.BSCChainName:
		return DefaultBSCGenesisBlock()
	case networkname.ChapelChainName:
		return DefaultChapelGenesisBlock()
	case networkname.RialtoChainName:
		return DefaultRialtoGenesisBlock()
	case networkname.MumbaiChainName:
		return DefaultMumbaiGenesisBlock()
	case networkname.BorMainnetChainName:
		return DefaultBorMainnetGenesisBlock()
	case networkname.BorDevnetChainName:
		return DefaultBorDevnetGenesisBlock()
	case networkname.GnosisChainName:
		return DefaultGnosisGenesisBlock()
	case networkname.ChiadoChainName:
		return DefaultChiadoGenesisBlock()
	default:
		return nil
	}
}
