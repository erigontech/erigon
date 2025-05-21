// Copyright 2014 The go-ethereum Authors
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

package core

import (
	"context"
	"crypto/ecdsa"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	params2 "github.com/erigontech/erigon/params"
)

//go:embed allocs
var allocs embed.FS

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New common.Hash
}

func (e *GenesisMismatchError) Error() string {
	config := params2.ChainConfigByGenesisHash(e.Stored)
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
//	db has no genesis |  main-net          |  genesis
//	db has genesis    |  from DB           |  genesis (if compatible)
//
// The stored chain configuration will be updated if it is compatible (i.e. does not
// specify a fork block below the local head block). In case of a conflict, the
// error is a *params.ConfigCompatError and the new, unwritten config is returned.
//
// The returned chain configuration is never nil.
func CommitGenesisBlock(db kv.RwDB, genesis *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	return CommitGenesisBlockWithOverride(db, genesis, nil, dirs, logger)
}

func CommitGenesisBlockWithOverride(db kv.RwDB, genesis *types.Genesis, overridePragueTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	c, b, err := WriteGenesisBlock(tx, genesis, overridePragueTime, dirs, logger)
	if err != nil {
		return c, b, err
	}
	err = tx.Commit()
	if err != nil {
		return c, b, err
	}
	return c, b, nil
}

func configOrDefault(g *types.Genesis, genesisHash common.Hash) *chain.Config {
	if g != nil {
		return g.Config
	}

	config := params2.ChainConfigByGenesisHash(genesisHash)
	if config != nil {
		return config
	} else {
		return params2.AllProtocolChanges
	}
}

func WriteGenesisBlock(tx kv.RwTx, genesis *types.Genesis, overridePragueTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	if err := WriteGenesisIfNotExist(tx, genesis); err != nil {
		return nil, nil, err
	}

	var storedBlock *types.Block
	if genesis != nil && genesis.Config == nil {
		return params2.AllProtocolChanges, nil, types.ErrGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	storedHash, storedErr := rawdb.ReadCanonicalHash(tx, 0)
	if storedErr != nil {
		return nil, nil, storedErr
	}

	applyOverrides := func(config *chain.Config) {
		if overridePragueTime != nil {
			config.PragueTime = overridePragueTime
		}
	}

	if (storedHash == common.Hash{}) {
		custom := true
		if genesis == nil {
			logger.Info("Writing main-net genesis block")
			genesis = MainnetGenesisBlock()
			custom = false
		}
		applyOverrides(genesis.Config)
		block, _, err1 := write(tx, genesis, dirs, logger)
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		if custom {
			logger.Info("Writing custom genesis block", "hash", block.Hash().String())
		}
		return genesis.Config, block, nil
	}

	// Check whether the genesis block is already written.
	if genesis != nil {
		block, _, err1 := GenesisToBlock(genesis, dirs, logger)
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		hash := block.Hash()
		if hash != storedHash {
			return genesis.Config, block, &GenesisMismatchError{Stored: storedHash, New: hash}
		}
	}
	number := rawdb.ReadHeaderNumber(tx, storedHash)
	if number != nil {
		var err error
		storedBlock, _, err = rawdb.ReadBlockWithSenders(tx, storedHash, *number)
		if err != nil {
			return genesis.Config, nil, err
		}
	}
	// Get the existing chain configuration.
	newCfg := configOrDefault(genesis, storedHash)
	applyOverrides(newCfg)
	if err := newCfg.CheckConfigForkOrder(); err != nil {
		return newCfg, nil, err
	}
	storedCfg, storedErr := ReadChainConfig(tx, storedHash)
	if storedErr != nil && newCfg.Bor == nil {
		return newCfg, nil, storedErr
	}
	if storedCfg == nil {
		logger.Warn("Found genesis block without chain config")
		err1 := WriteChainConfig(tx, storedHash, newCfg)
		if err1 != nil {
			return newCfg, nil, err1
		}
		return newCfg, storedBlock, nil
	}
	// Special case: don't change the existing config of a private chain if no new
	// config is supplied. This is useful, for example, to preserve DB config created by erigon init.
	// In that case, only apply the overrides.
	if genesis == nil && params2.ChainConfigByGenesisHash(storedHash) == nil {
		newCfg = storedCfg
		applyOverrides(newCfg)
	}
	// Check config compatibility and write the config. Compatibility errors
	// are returned to the caller unless we're already at block zero.
	height := rawdb.ReadHeaderNumber(tx, rawdb.ReadHeadHeaderHash(tx))
	if height != nil {
		compatibilityErr := storedCfg.CheckCompatible(newCfg, *height)
		if compatibilityErr != nil && *height != 0 && compatibilityErr.RewindTo != 0 {
			return newCfg, storedBlock, compatibilityErr
		}
	}
	if err := WriteChainConfig(tx, storedHash, newCfg); err != nil {
		return newCfg, nil, err
	}
	return newCfg, storedBlock, nil
}

func WriteGenesisState(g *types.Genesis, tx kv.RwTx, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err := GenesisToBlock(g, dirs, logger)
	if err != nil {
		return nil, nil, err
	}

	var stateWriter state.StateWriter
	stateWriter = state.NewNoopWriter()

	if block.Number().Sign() != 0 {
		return nil, statedb, errors.New("can't commit genesis block with number > 0")
	}
	if err := statedb.CommitBlock(&chain.Rules{}, stateWriter); err != nil {
		return nil, statedb, fmt.Errorf("cannot write state: %w", err)
	}

	return block, statedb, nil
}

func MustCommitGenesis(g *types.Genesis, db kv.RwDB, dirs datadir.Dirs, logger log.Logger) *types.Block {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	block, _, err := write(tx, g, dirs, logger)
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	return block
}

// Write writes the block and state of a genesis specification to the database.
// The block is committed as the canonical head block.
func write(tx kv.RwTx, g *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err2 := WriteGenesisState(g, tx, dirs, logger)
	if err2 != nil {
		return block, statedb, err2
	}
	config := g.Config
	if config == nil {
		config = params2.AllProtocolChanges
	}
	if err := config.CheckConfigForkOrder(); err != nil {
		return nil, nil, err
	}

	if err := rawdb.WriteBlock(tx, block); err != nil {
		return nil, nil, err
	}
	if err := rawdb.WriteTd(tx, block.Hash(), block.NumberU64(), g.Difficulty); err != nil {
		return nil, nil, err
	}
	if err := rawdbv3.TxNums.Append(tx, 0, uint64(block.Transactions().Len()+1)); err != nil {
		return nil, nil, err
	}

	if err := rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64()); err != nil {
		return nil, nil, err
	}

	rawdb.WriteHeadBlockHash(tx, block.Hash())
	if err := rawdb.WriteHeadHeaderHash(tx, block.Hash()); err != nil {
		return nil, nil, err
	}
	if err := WriteChainConfig(tx, block.Hash(), config); err != nil {
		return nil, nil, err
	}

	return block, statedb, nil
}

// GenesisBlockForTesting creates and writes a block in which addr has the given wei balance.
func GenesisBlockForTesting(db kv.RwDB, addr common.Address, balance *big.Int, dirs datadir.Dirs, logger log.Logger) *types.Block {
	g := types.Genesis{Alloc: types.GenesisAlloc{addr: {Balance: balance}}, Config: chain.TestChainConfig}
	block := MustCommitGenesis(&g, db, dirs, logger)
	return block
}

type GenAccount struct {
	Addr    common.Address
	Balance *big.Int
}

func GenesisWithAccounts(db kv.RwDB, accs []GenAccount, dirs datadir.Dirs, logger log.Logger) *types.Block {
	g := types.Genesis{Config: chain.TestChainConfig}
	allocs := make(map[common.Address]types.GenesisAccount)
	for _, acc := range accs {
		allocs[acc.Addr] = types.GenesisAccount{Balance: acc.Balance}
	}
	g.Alloc = allocs
	block := MustCommitGenesis(&g, db, dirs, logger)
	return block
}

// MainnetGenesisBlock returns the Ethereum main net genesis block.
func MainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.MainnetChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa"),
		GasLimit:   5000,
		Difficulty: big.NewInt(17179869184),
		Alloc:      readPrealloc("allocs/mainnet.json"),
	}
}

// HoleskyGenesisBlock returns the Holesky main net genesis block.
func HoleskyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.HoleskyChainConfig,
		Nonce:      4660,
		GasLimit:   25000000,
		Difficulty: big.NewInt(1),
		Timestamp:  1695902100,
		Alloc:      readPrealloc("allocs/holesky.json"),
	}
}

// SepoliaGenesisBlock returns the Sepolia network genesis block.
func SepoliaGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.SepoliaChainConfig,
		Nonce:      0,
		ExtraData:  []byte("Sepolia, Athens, Attica, Greece!"),
		GasLimit:   30000000,
		Difficulty: big.NewInt(131072),
		Timestamp:  1633267481,
		Alloc:      readPrealloc("allocs/sepolia.json"),
	}
}

// HoodiGenesisBlock returns the Hoodi network genesis block.
func HoodiGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.HoodiChainConfig,
		Nonce:      0x1234,
		ExtraData:  []byte(""),
		GasLimit:   0x2255100, // 36M
		Difficulty: big.NewInt(1),
		Timestamp:  1742212800,
		Alloc:      readPrealloc("allocs/hoodi.json"),
	}
}

// AmoyGenesisBlock returns the Amoy network genesis block.
func AmoyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.AmoyChainConfig,
		Nonce:      0,
		Timestamp:  1700225065,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/amoy.json"),
	}
}

// BorMainnetGenesisBlock returns the Bor Mainnet network genesis block.
func BorMainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.BorMainnetChainConfig,
		Nonce:      0,
		Timestamp:  1590824836,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_mainnet.json"),
	}
}

func BorDevnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.BorDevnetChainConfig,
		Nonce:      0,
		Timestamp:  1558348305,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   common.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_devnet.json"),
	}
}

func GnosisGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.GnosisChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/gnosis.json"),
	}
}

func ChiadoGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params2.ChiadoChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/chiado.json"),
	}
}
func TestGenesisBlock() *types.Genesis {
	return &types.Genesis{Config: chain.TestChainConfig}
}

// Pre-calculated version of:
//
//	DevnetSignPrivateKey = crypto.HexToECDSA(sha256.Sum256([]byte("erigon devnet key")))
//	DevnetEtherbase=crypto.PubkeyToAddress(DevnetSignPrivateKey.PublicKey)
var DevnetSignPrivateKey, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")
var DevnetEtherbase = common.HexToAddress("67b1d87101671b127f5f8714789c7192f7ad340e")

// DevnetSignKey is defined like this to allow the devnet process to pre-allocate keys
// for nodes and then pass the address via --miner.etherbase - the function will be called
// to retieve the mining key
var DevnetSignKey = func(address common.Address) *ecdsa.PrivateKey {
	return DevnetSignPrivateKey
}

// DeveloperGenesisBlock returns the 'geth --dev' genesis block.
func DeveloperGenesisBlock(period uint64, faucet common.Address) *types.Genesis {
	// Override the default period to the user requested one
	var config chain.Config
	copier.Copy(&config, params2.AllCliqueProtocolChanges)
	config.Clique.Period = period

	// Assemble and return the genesis with the precompiles and faucet pre-funded
	return &types.Genesis{
		Config:     &config,
		ExtraData:  append(append(make([]byte, 32), faucet[:]...), make([]byte, crypto.SignatureLength)...),
		GasLimit:   11500000,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/dev.json"),
	}
}

// ToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func GenesisToBlock(g *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	if dirs.SnapDomain == "" {
		panic("empty `dirs` variable")
	}
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
		BlobGasUsed:   g.BlobGasUsed,
		ExcessBlobGas: g.ExcessBlobGas,
		RequestsHash:  g.RequestsHash,
	}
	if g.AuRaSeal != nil && len(g.AuRaSeal.AuthorityRound.Signature) > 0 {
		head.AuRaSeal = g.AuRaSeal.AuthorityRound.Signature
		head.AuRaStep = uint64(g.AuRaSeal.AuthorityRound.Step)
	}
	if g.GasLimit == 0 {
		head.GasLimit = params.GenesisGasLimit
	}
	if g.Difficulty == nil {
		head.Difficulty = params.GenesisDifficulty
	}
	if g.Config != nil && g.Config.IsLondon(0) {
		if g.BaseFee != nil {
			head.BaseFee = g.BaseFee
		} else {
			head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
		}
	}

	var withdrawals []*types.Withdrawal
	if g.Config != nil && g.Config.IsShanghai(g.Timestamp) {
		withdrawals = []*types.Withdrawal{}
	}

	if g.Config != nil && g.Config.IsCancun(g.Timestamp) {
		if g.BlobGasUsed != nil {
			head.BlobGasUsed = g.BlobGasUsed
		} else {
			head.BlobGasUsed = new(uint64)
		}
		if g.ExcessBlobGas != nil {
			head.ExcessBlobGas = g.ExcessBlobGas
		} else {
			head.ExcessBlobGas = new(uint64)
		}
		if g.ParentBeaconBlockRoot != nil {
			head.ParentBeaconBlockRoot = g.ParentBeaconBlockRoot
		} else {
			head.ParentBeaconBlockRoot = &common.Hash{}
		}
	}

	if g.Config != nil && g.Config.IsPrague(g.Timestamp) {
		if g.RequestsHash != nil {
			head.RequestsHash = g.RequestsHash
		} else {
			head.RequestsHash = &empty.RequestsHash
		}
	}

	var root common.Hash
	var statedb *state.IntraBlockState // reader behind this statedb is dead at the moment of return, tx is rolled back

	ctx := context.Background()
	wg, ctx := errgroup.WithContext(ctx)
	// we may run inside write tx, can't open 2nd write tx in same goroutine
	wg.Go(func() error {
		// some users creaing > 1Gb custome genesis by `erigon init`
		genesisTmpDB := mdbx.New(kv.TemporaryDB, logger).InMem(dirs.DataDir).MapSize(2 * datasize.GB).GrowthStep(1 * datasize.MB).MustOpen()
		defer genesisTmpDB.Close()

		salt, err := state2.GetStateIndicesSalt(dirs, false, logger)
		if err != nil {
			return err
		}
		agg, err := state2.NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, genesisTmpDB, logger)
		if err != nil {
			return err
		}
		defer agg.Close()

		tdb, err := temporal.New(genesisTmpDB, agg)
		if err != nil {
			return err
		}
		defer tdb.Close()

		tx, err := tdb.BeginTemporalRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		sd, err := state2.NewSharedDomains(tx, logger)
		if err != nil {
			return err
		}
		defer sd.Close()

		//r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)
		r, w := state.NewReaderV3(sd), state.NewWriter(sd, nil)
		statedb = state.New(r)
		statedb.SetTrace(false)

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
			statedb.AddBalance(addr, balance, tracing.BalanceIncreaseGenesisBalance)
			statedb.SetCode(addr, account.Code)
			statedb.SetNonce(addr, account.Nonce)
			for key, value := range account.Storage {
				key := key
				val := uint256.NewInt(0).SetBytes(value.Bytes())
				statedb.SetState(addr, key, *val)
			}

			if len(account.Constructor) > 0 {
				if _, err = SysCreate(addr, account.Constructor, g.Config, statedb, head); err != nil {
					return err
				}
			}

			if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
				statedb.SetIncarnation(addr, state.FirstContractIncarnation)
			}
		}
		if err = statedb.FinalizeTx(&chain.Rules{}, w); err != nil {
			return err
		}

		rh, err := sd.ComputeCommitment(context.Background(), true, 0, "genesis")
		if err != nil {
			return err
		}
		root = common.BytesToHash(rh)
		return nil
	})

	if err := wg.Wait(); err != nil {
		return nil, nil, err
	}

	head.Root = root

	return types.NewBlock(head, nil, nil, nil, withdrawals), statedb, nil
}

func sortedAllocKeys(m types.GenesisAlloc) []string {
	keys := make([]string, len(m))
	i := 0
	for k := range m {
		keys[i] = string(k.Bytes())
		i++
	}
	slices.Sort(keys)
	return keys
}

func readPrealloc(filename string) types.GenesisAlloc {
	f, err := allocs.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Could not open genesis preallocation for %s: %v", filename, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	ga := make(types.GenesisAlloc)
	err = decoder.Decode(&ga)
	if err != nil {
		panic(fmt.Sprintf("Could not parse genesis preallocation for %s: %v", filename, err))
	}
	return ga
}

func GenesisBlockByChainName(chain string) *types.Genesis {
	switch chain {
	case networkname.Mainnet:
		return MainnetGenesisBlock()
	case networkname.Holesky:
		return HoleskyGenesisBlock()
	case networkname.Sepolia:
		return SepoliaGenesisBlock()
	case networkname.Hoodi:
		return HoodiGenesisBlock()
	case networkname.Amoy:
		return AmoyGenesisBlock()
	case networkname.BorMainnet:
		return BorMainnetGenesisBlock()
	case networkname.BorDevnet:
		return BorDevnetGenesisBlock()
	case networkname.Gnosis:
		return GnosisGenesisBlock()
	case networkname.Chiado:
		return ChiadoGenesisBlock()
	case networkname.Test:
		return TestGenesisBlock()
	default:
		return nil
	}
}
