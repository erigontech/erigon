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

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"
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
	SealRlp    []byte              `json:"sealRlp"`

	// These fields are used for consensus tests. Please don't use them
	// in actual genesis blocks.
	Number     uint64      `json:"number"`
	GasUsed    uint64      `json:"gasUsed"`
	ParentHash common.Hash `json:"parentHash"`
	BaseFee    *big.Int    `json:"baseFee"`
}

// GenesisAlloc specifies the initial state that is part of the genesis block.
type GenesisAlloc map[common.Address]GenesisAccount

type AuthorityRoundSeal struct {
	/// Seal step.
	Step uint64 `json:"step"`
	/// Seal signature.
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
	BaseFee    *math.HexOrDecimal256
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

// CommitGenesisBlock writes or updates the genesis block in db.
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
func CommitGenesisBlock(db kv.RwDB, genesis *Genesis) (*params.ChainConfig, *types.Block, error) {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	c, b, err := WriteGenesisBlock(tx, genesis)
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

func OverrideGenesisBlock(db kv.RwTx, genesis *Genesis) (*params.ChainConfig, *types.Block, error) {
	stored, err := rawdb.ReadCanonicalHash(db, 0)
	if err != nil {
		return nil, nil, err
	}
	err = rawdb.DeleteCanonicalHash(db, 0)
	if err != nil {
		return nil, nil, err
	}
	err = rawdb.DeleteChainConfig(db, stored)
	if err != nil {
		return nil, nil, err
	}
	return WriteGenesisBlock(db, genesis)
}

func WriteGenesisBlock(db kv.RwTx, genesis *Genesis) (*params.ChainConfig, *types.Block, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllEthashProtocolChanges, nil, ErrGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	stored, storedErr := rawdb.ReadCanonicalHash(db, 0)
	if storedErr != nil {
		return nil, nil, storedErr
	}
	if (stored == common.Hash{}) {
		custom := true
		if genesis == nil {
			log.Info("Writing default main-net genesis block")
			genesis = DefaultGenesisBlock()
			custom = false
		}
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
		if hash != stored {
			return genesis.Config, block, &GenesisMismatchError{stored, hash}
		}
	}
	storedBlock, err := rawdb.ReadBlockByHash(db, stored)
	if err != nil {
		return genesis.Config, nil, err
	}
	// Get the existing chain configuration.
	newcfg := genesis.configOrDefault(stored)
	if err := newcfg.CheckConfigForkOrder(); err != nil {
		return newcfg, nil, err
	}
	storedcfg, storedErr := rawdb.ReadChainConfig(db, stored)
	if storedErr != nil {
		return newcfg, nil, storedErr
	}
	if storedcfg == nil {
		log.Warn("Found genesis block without chain config")
		err1 := rawdb.WriteChainConfig(db, stored, newcfg)
		if err1 != nil {
			return newcfg, nil, err1
		}
		return newcfg, storedBlock, nil
	}
	// Special case: don't change the existing config of a non-mainnet chain if no new
	// config is supplied. These chains would get AllProtocolChanges (and a compat error)
	// if we just continued here.
	if genesis == nil && stored != params.MainnetGenesisHash {
		return storedcfg, storedBlock, nil
	}
	// Check config compatibility and write the config. Compatibility errors
	// are returned to the caller unless we're already at block zero.
	height := rawdb.ReadHeaderNumber(db, rawdb.ReadHeadHeaderHash(db))
	if height == nil {
		//return newcfg, stored, stateDB, fmt.Errorf("missing block number for head header hash")
	} else {
		compatErr := storedcfg.CheckCompatible(newcfg, *height)
		if compatErr != nil && *height != 0 && compatErr.RewindTo != 0 {
			return newcfg, storedBlock, compatErr
		}
	}
	if err := rawdb.WriteChainConfig(db, stored, newcfg); err != nil {
		return newcfg, nil, err
	}
	return newcfg, storedBlock, nil
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
	case ghash == params.ErigonGenesisHash:
		return params.ErigonChainConfig
	case ghash == params.CalaverasGenesisHash:
		return params.CalaverasChainConfig
	case ghash == params.SokolGenesisHash:
		return params.SokolChainConfig
	default:
		return params.AllEthashProtocolChanges
	}
}

// ToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func (g *Genesis) ToBlock() (*types.Block, *state.IntraBlockState, error) {
	var root common.Hash
	var statedb *state.IntraBlockState
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() { // we may run inside write tx, can't open 2nd write tx in same goroutine
		defer wg.Done()
		tmpDB := mdbx.NewMDBX(log.New()).InMem().MustOpen()
		defer tmpDB.Close()
		tx, err := tmpDB.BeginRw(context.Background())
		if err != nil {
			panic(err)
		}
		defer tx.Rollback()
		r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)
		statedb = state.New(r)
		for addr, account := range g.Alloc {
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

			if len(account.Code) > 0 || len(account.Storage) > 0 {
				statedb.SetIncarnation(addr, 1)
			}
		}
		if err := statedb.FinalizeTx(params.Rules{}, w); err != nil {
			panic(err)
		}
		root, err = trie.CalcRoot("genesis", tx)
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()
	decodeSeal := func(in []byte) (seal []rlp.RawValue) {
		if len(in) == 0 {
			return nil
		}
		err := rlp.Decode(bytes.NewReader(in), &seal)
		if err != nil {
			panic(err)
		}
		return seal
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
		BaseFee:    g.BaseFee,
		Seal:       decodeSeal(g.SealRlp),
		WithSeal:   g.SealRlp != nil,
	}
	if g.GasLimit == 0 {
		head.GasLimit = params.GenesisGasLimit
	}
	if g.Difficulty == nil {
		head.Difficulty = params.GenesisDifficulty
	}
	if g.Config != nil && (g.Config.IsLondon(0)) {
		head.Eip1559 = true
		if g.BaseFee != nil {
			head.BaseFee = g.BaseFee
		} else {
			head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
		}
	}

	return types.NewBlock(head, nil, nil, nil), statedb, nil
}

func (g *Genesis) WriteGenesisState(tx kv.RwTx) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err := g.ToBlock()
	if err != nil {
		return nil, nil, err
	}
	for addr, account := range g.Alloc {
		if len(account.Code) == 0 && len(account.Storage) > 0 {
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

	if err := statedb.CommitBlock(params.Rules{}, blockWriter); err != nil {
		return nil, statedb, fmt.Errorf("cannot write state: %v", err)
	}
	if err := blockWriter.WriteChangeSets(); err != nil {
		return nil, statedb, fmt.Errorf("cannot write change sets: %v", err)
	}
	if err := blockWriter.WriteHistory(); err != nil {
		return nil, statedb, fmt.Errorf("cannot write history: %v", err)
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
		config = params.AllEthashProtocolChanges
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
	return block, statedb, nil
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

func DefaultErigonGenesisBlock() *Genesis {
	return &Genesis{
		Config:     params.ErigonChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x3535353535353535353535353535353535353535353535353535353535353535"),
		GasLimit:   1000000000,
		Difficulty: big.NewInt(1048576),
		Alloc:      readPrealloc("allocs/erigonmine.json"),
	}
}

func DefaultCalaverasGenesisBlock() *Genesis {
	// Full genesis: https://github.com/ethereum/eth1.0-specs/blob/master/network-upgrades/client-integration-testnets/aleut.md
	return &Genesis{
		Config:     params.CalaverasChainConfig,
		Timestamp:  0x60b3877f,
		ExtraData:  hexutil.MustDecode("0x00000000000000000000000000000000000000000000000000000000000000005211cea3870c7ba7c6c44b185e62eecdb864cd8c560228ce57d31efbf64c200b2c200aacec78cf17a7148e784fe95a7a750335f8b9572ee28d72e7650000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x47b760,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/calaveras.json"),
	}
}

func DefaultSokolGenesisBlock() *Genesis {
	/*
		header rlp: f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
	*/
	sealRlp, err := rlp.EncodeToBytes([][]byte{
		common.FromHex(""),
		common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
	})
	if err != nil {
		panic(err)
	}
	return &Genesis{
		Config:     params.SokolChainConfig,
		Timestamp:  0x0,
		SealRlp:    sealRlp,
		GasLimit:   0x663BE0,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/sokol.json"),
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
