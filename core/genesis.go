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

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
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

	// These fields are used for consensus tests. Please don't use them
	// in actual genesis blocks.
	Number     uint64      `json:"number"`
	GasUsed    uint64      `json:"gasUsed"`
	ParentHash common.Hash `json:"parentHash"`
}

// GenesisAlloc specifies the initial state that is part of the genesis block.
type GenesisAlloc map[common.Address]GenesisAccount

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
type GenesisAccount struct {
	Code       []byte                      `json:"code,omitempty"`
	Storage    map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance    *big.Int                    `json:"balance" gencodec:"required"`
	Nonce      uint64                      `json:"nonce,omitempty"`
	PrivateKey []byte                      `json:"secretKey,omitempty"` // for tests
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
	Alloc      map[common.UnprefixedAddress]GenesisAccount
}

type genesisAccountMarshaling struct {
	Code       hexutil.Bytes
	Balance    *math.HexOrDecimal256
	Nonce      math.HexOrDecimal64
	Storage    map[storageJSON]storageJSON
	PrivateKey hexutil.Bytes
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
	return fmt.Sprintf("database contains incompatible genesis (have %x, new %x)", e.Stored, e.New)
}

// SetupGenesisBlock writes or updates the genesis block in db.
// The block that will be used is:
//
//                          genesis == nil       genesis != nil
//                       +------------------------------------------
//     db has no genesis |  main-net default  |  genesis
//     db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration is never nil.
func SetupGenesisBlock(db ethdb.Database, genesis *Genesis, history bool, overwrite bool) (*params.ChainConfig, common.Hash, error) {
	return SetupGenesisBlockWithOverride(db, genesis, nil, history, overwrite)
}

func SetupGenesisBlockWithOverride(db ethdb.Database, genesis *Genesis, overrideBerlin *big.Int, history bool, overwrite bool) (*params.ChainConfig, common.Hash, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllEthashProtocolChanges, common.Hash{}, ErrGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	stored, storedErr := rawdb.ReadCanonicalHash(db, 0)
	if storedErr != nil {
		return nil, common.Hash{}, storedErr
	}
	if overwrite || (stored == common.Hash{}) {
		custom := true
		if genesis == nil {
			log.Info("Writing default main-net genesis block")
			genesis = DefaultGenesisBlock()
			custom = false
		}
		block, _, err1 := genesis.Commit(db, history)
		if err1 != nil {
			return genesis.Config, common.Hash{}, err1
		}
		if custom {
			log.Info("Writing custom genesis block", "hash", block.Hash().String())
		}
		return genesis.Config, block.Hash(), nil
	}
	// Check whether the genesis block is already written.
	if genesis != nil {
		block, _, err1 := genesis.ToBlock(history)
		if err1 != nil {
			return genesis.Config, common.Hash{}, err1
		}
		hash := block.Hash()
		if hash != stored {
			return genesis.Config, block.Hash(), &GenesisMismatchError{stored, hash}
		}
	}
	// Get the existing chain configuration.
	newcfg := genesis.configOrDefault(stored)
	if overrideBerlin != nil {
		newcfg.BerlinBlock = overrideBerlin
	}
	if err := newcfg.CheckConfigForkOrder(); err != nil {
		return newcfg, common.Hash{}, err
	}
	storedcfg, storedErr := rawdb.ReadChainConfig(db, stored)
	if storedErr != nil {
		return newcfg, common.Hash{}, storedErr
	}
	if overwrite || storedcfg == nil {
		log.Warn("Found genesis block without chain config")
		err1 := rawdb.WriteChainConfig(db, stored, newcfg)
		if err1 != nil {
			return newcfg, common.Hash{}, err1
		}
		return newcfg, stored, nil
	}
	// Special case: don't change the existing config of a non-mainnet chain if no new
	// config is supplied. These chains would get AllProtocolChanges (and a compat error)
	// if we just continued here.
	if genesis == nil && stored != params.MainnetGenesisHash {
		return storedcfg, stored, nil
	}
	// Check config compatibility and write the config. Compatibility errors
	// are returned to the caller unless we're already at block zero.
	height := rawdb.ReadHeaderNumber(db, rawdb.ReadHeadHeaderHash(db))
	if height == nil {
		//return newcfg, stored, stateDB, fmt.Errorf("missing block number for head header hash")
	} else {
		compatErr := storedcfg.CheckCompatible(newcfg, *height)
		if compatErr != nil && *height != 0 && compatErr.RewindTo != 0 {
			return newcfg, stored, compatErr
		}
	}
	if err := rawdb.WriteChainConfig(db, stored, newcfg); err != nil {
		return newcfg, common.Hash{}, err
	}
	return newcfg, stored, nil
}

func (g *Genesis) configOrDefault(ghash common.Hash) *params.ChainConfig {
	switch {
	case g != nil:
		return g.Config
	case ghash == params.MainnetGenesisHash:
		return params.MainnetChainConfig
	case ghash == params.RopstenGenesisHash:
		return params.RopstenChainConfig
	case ghash == params.RinkebyGenesisHash:
		return params.RinkebyChainConfig
	case ghash == params.GoerliGenesisHash:
		return params.GoerliChainConfig
	case ghash == params.YoloV3GenesisHash:
		return params.YoloV3ChainConfig
	case ghash == params.TurboMineGenesisHash:
		return params.TurboMineChainConfig
	default:
		return params.AllEthashProtocolChanges
	}
}

// ToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func (g *Genesis) ToBlock(history bool) (*types.Block, *state.IntraBlockState, error) {
	tmpDB := ethdb.NewMemDatabase()
	defer tmpDB.Close()
	tx, err := tmpDB.Begin(context.Background(), ethdb.RW)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)
	statedb := state.New(r)
	for addr, account := range g.Alloc {
		balance, _ := uint256.FromBig(account.Balance)
		statedb.AddBalance(addr, balance)
		statedb.SetCode(addr, account.Code)
		statedb.SetNonce(addr, account.Nonce)
		for key, value := range account.Storage {
			key := key
			val := uint256.NewInt().SetBytes(value.Bytes())
			statedb.SetState(addr, &key, *val)
		}

		if len(account.Code) > 0 || len(account.Storage) > 0 {
			statedb.SetIncarnation(addr, 1)
		}
	}
	if err := statedb.FinalizeTx(context.Background(), w); err != nil {
		return nil, nil, err
	}
	root, err := trie.CalcRoot("genesis", tx)
	if err != nil {
		return nil, nil, err
	}
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
		Root:       root,
	}
	if g.GasLimit == 0 {
		head.GasLimit = params.GenesisGasLimit
	}
	if g.Difficulty == nil {
		head.Difficulty = params.GenesisDifficulty
	}
	if g.Config != nil && g.Config.IsAleut(0) {
		head.Eip1559 = true
		head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
	}

	return types.NewBlock(head, nil, nil, nil), statedb, nil
}

func (g *Genesis) WriteGenesisState(tx ethdb.Database, history bool) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err := g.ToBlock(history)
	if err != nil {
		return nil, nil, err
	}
	for addr, account := range g.Alloc {
		if len(account.Code) == 0 && len(account.Storage) > 0 {
			// Special case for weird tests - inaccessible storage
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], state.FirstContractIncarnation)
			if err := tx.Put(dbutils.IncarnationMapBucket, addr[:], b[:]); err != nil {
				return nil, nil, err
			}
		}
	}

	if block.Number().Sign() != 0 {
		return nil, statedb, fmt.Errorf("can't commit genesis block with number > 0")
	}

	blockWriter := state.NewPlainStateWriter(tx, tx, 0)

	if err := statedb.CommitBlock(context.Background(), blockWriter); err != nil {
		return nil, statedb, fmt.Errorf("cannot write state: %v", err)
	}

	if err := blockWriter.WriteChangeSets(); err != nil {
		return nil, statedb, fmt.Errorf("cannot write change sets: %v", err)
	}
	// Optionally write history
	if history {
		if err := blockWriter.WriteHistory(); err != nil {
			return nil, statedb, fmt.Errorf("cannot write history: %v", err)
		}
	}
	return block, statedb, nil
}

// Commit writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func (g *Genesis) Commit(db ethdb.Database, history bool) (*types.Block, *state.IntraBlockState, error) {
	tx, dbErr := db.Begin(context.Background(), ethdb.RW)
	if dbErr != nil {
		return nil, nil, dbErr
	}
	block, statedb, err2 := g.WriteGenesisState(tx, history)
	if err2 != nil {
		return block, statedb, err2
	}
	config := g.Config
	if config == nil {
		config = params.AllEthashProtocolChanges
	}
	if err := config.CheckConfigForkOrder(); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteTd(tx, block.Hash(), block.NumberU64(), g.Difficulty); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteBlockDeprecated(context.Background(), tx, block); err != nil {
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

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	return block, statedb, nil
}

// MustCommit writes the genesis block and state to db, panicking on error.
// The block is committed as the canonical head block.
func (g *Genesis) MustCommit(db ethdb.Database) *types.Block {
	block, _, err := g.Commit(db, true /* history */)
	if err != nil {
		panic(err)
	}
	return block
}

// GenesisBlockForTesting creates and writes a block in which addr has the given wei balance.
func GenesisBlockForTesting(db ethdb.Database, addr common.Address, balance *big.Int) *types.Block {
	g := Genesis{Alloc: GenesisAlloc{addr: {Balance: balance}}, Config: params.TestChainConfig}
	block := g.MustCommit(db)
	return block
}

type GenAccount struct {
	Addr    common.Address
	Balance *big.Int
}

func GenesisWithAccounts(db ethdb.Database, accs []GenAccount) *types.Block {
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

func DefaultYoloV3GenesisBlock() *Genesis {
	// Full genesis: https://gist.github.com/holiman/c6ed9269dce28304ad176314caa75e97
	return &Genesis{
		Config:     params.YoloV3ChainConfig,
		Timestamp:  0x6027dd2e,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000001041afbcb359d5a8dc58c15b2ff51354ff8a217d0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x47b760,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/yolov3.json"),
	}
}

func DefaultTurboMineGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.TurboMineChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x3535353535353535353535353535353535353535353535353535353535353535"),
		GasLimit:   1000000000,
		Difficulty: big.NewInt(1048576),
		Alloc:      readPrealloc("allocs/turbomine.json"),
	}
}

func DefaultAleutGenesisBlock() *Genesis {
	// Full genesis: https://github.com/ethereum/eth1.0-specs/blob/master/network-upgrades/client-integration-testnets/aleut.md
	return &Genesis{
		Config:     params.AleutChainConfig,
		Timestamp:  0,
		ExtraData:  hexutil.MustDecode("0x000000000000000000000000000000000000000000000000000000000000000036267c845cc42b57ccb869d655e5d5fb620cc69a0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x1312D00,
		Difficulty: big.NewInt(0x400),
		Alloc:      readPrealloc("allocs/aleut.json"),
	}
}

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
		Alloc: map[common.Address]GenesisAccount{
			common.BytesToAddress([]byte{1}): {Balance: big.NewInt(1)}, // ECRecover
			common.BytesToAddress([]byte{2}): {Balance: big.NewInt(1)}, // SHA256
			common.BytesToAddress([]byte{3}): {Balance: big.NewInt(1)}, // RIPEMD
			common.BytesToAddress([]byte{4}): {Balance: big.NewInt(1)}, // Identity
			common.BytesToAddress([]byte{5}): {Balance: big.NewInt(1)}, // ModExp
			common.BytesToAddress([]byte{6}): {Balance: big.NewInt(1)}, // ECAdd
			common.BytesToAddress([]byte{7}): {Balance: big.NewInt(1)}, // ECScalarMul
			common.BytesToAddress([]byte{8}): {Balance: big.NewInt(1)}, // ECPairing
			common.BytesToAddress([]byte{9}): {Balance: big.NewInt(1)}, // BLAKE2b
			faucet:                           {Balance: new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(9))},
		},
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
