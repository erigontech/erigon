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

package genesiswrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sort"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/params"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
	polygonchain "github.com/erigontech/erigon/polygon/chain"
)

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New common.Hash
}

func (e *GenesisMismatchError) Error() string {
	var advice string
	spec, err := chainspec.ChainSpecByGenesisHash(e.Stored)
	if err == nil {
		advice = fmt.Sprintf(" (try with flag --chain=%s)", spec.Name)
	}
	return fmt.Sprintf("database contains genesis (have %x, new %x)", e.Stored, e.New) + advice
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

func CommitGenesisBlockWithOverride(db kv.RwDB, genesis *types.Genesis, overrideOsakaTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	c, b, err := WriteGenesisBlock(tx, genesis, overrideOsakaTime, dirs, logger)
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
	spec, err := chainspec.ChainSpecByGenesisHash(genesisHash)
	if err != nil {
		return chain.AllProtocolChanges
	}
	return spec.Config
}

func WriteGenesisBlock(tx kv.RwTx, genesis *types.Genesis, overrideOsakaTime *big.Int, dirs datadir.Dirs, logger log.Logger) (*chain.Config, *types.Block, error) {
	if err := rawdb.WriteGenesisIfNotExist(tx, genesis); err != nil {
		return nil, nil, err
	}

	var storedBlock *types.Block
	if genesis != nil && genesis.Config == nil {
		return chain.AllProtocolChanges, nil, types.ErrGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	storedHash, storedErr := rawdb.ReadCanonicalHash(tx, 0)
	if storedErr != nil {
		return nil, nil, storedErr
	}

	applyOverrides := func(config *chain.Config) {
		if overrideOsakaTime != nil {
			config.OsakaTime = overrideOsakaTime
		}
	}

	if (storedHash == common.Hash{}) {
		custom := true
		if genesis == nil {
			logger.Info("Writing main-net genesis block")
			genesis = chainspec.MainnetGenesisBlock()
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
	storedCfg, storedErr := rawdb.ReadChainConfig(tx, storedHash)
	if storedErr != nil && newCfg.Bor == nil {
		return newCfg, nil, storedErr
	}
	if storedCfg == nil {
		logger.Warn("Found genesis block without chain config")
		err1 := rawdb.WriteChainConfig(tx, storedHash, newCfg)
		if err1 != nil {
			return newCfg, nil, err1
		}
		return newCfg, storedBlock, nil
	}
	// Special case: don't change the existing config of a private chain if no new
	// config is supplied. This is useful, for example, to preserve DB config created by erigon init.
	// In that case, only apply the overrides.
	if genesis == nil {
		if _, err := chainspec.ChainSpecByGenesisHash(storedHash); err != nil {
			newCfg = storedCfg
			applyOverrides(newCfg)
		}
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
	if err := rawdb.WriteChainConfig(tx, storedHash, newCfg); err != nil {
		return newCfg, nil, err
	}
	return newCfg, storedBlock, nil
}

func WriteGenesisState(g *types.Genesis, tx kv.RwTx, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	block, statedb, err := GenesisToBlock(g, dirs, logger)
	if err != nil {
		return nil, nil, err
	}

	stateWriter := state.NewNoopWriter()

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
	block, statedb, err := WriteGenesisState(g, tx, dirs, logger)
	if err != nil {
		return block, statedb, err
	}
	err = WriteGenesisBesideState(block, tx, g)
	return block, statedb, err
}

// Write writes the block a genesis specification to the database.
// The block is committed as the canonical head block.
func WriteGenesisBesideState(block *types.Block, tx kv.RwTx, g *types.Genesis) error {
	config := g.Config
	if config == nil {
		config = chain.AllProtocolChanges
	}
	if err := config.CheckConfigForkOrder(); err != nil {
		return err
	}

	if err := rawdb.WriteBlock(tx, block); err != nil {
		return err
	}
	if err := rawdb.WriteTd(tx, block.Hash(), block.NumberU64(), g.Difficulty); err != nil {
		return err
	}
	if err := rawdbv3.TxNums.Append(tx, 0, uint64(block.Transactions().Len()+1)); err != nil {
		return err
	}

	if err := rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64()); err != nil {
		return err
	}

	rawdb.WriteHeadBlockHash(tx, block.Hash())
	if err := rawdb.WriteHeadHeaderHash(tx, block.Hash()); err != nil {
		return err
	}
	return rawdb.WriteChainConfig(tx, block.Hash(), config)
}

// GenesisToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func GenesisToBlock(g *types.Genesis, dirs datadir.Dirs, logger log.Logger) (*types.Block, *state.IntraBlockState, error) {
	if dirs.SnapDomain == "" {
		panic("empty `dirs` variable")
	}
	_ = g.Alloc //nil-check

	head, withdrawals := GenesisWithoutStateToBlock(g)

	var root common.Hash
	var statedb *state.IntraBlockState // reader behind this statedb is dead at the moment of return, tx is rolled back

	ctx := context.Background()
	wg, ctx := errgroup.WithContext(ctx)
	// we may run inside write tx, can't open 2nd write tx in same goroutine
	wg.Go(func() (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("panic: %v, %s", rec, dbg.Stack())
			}
		}()
		// some users creating > 1Gb custome genesis by `erigon init`
		genesisTmpDB := mdbx.New(kv.TemporaryDB, logger).InMem(dirs.Tmp).MapSize(2 * datasize.GB).GrowthStep(1 * datasize.MB).MustOpen()
		defer genesisTmpDB.Close()

		salt, err := dbstate.GetStateIndicesSalt(dirs, false, logger)
		if err != nil {
			return err
		}
		agg, err := dbstate.NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, salt, genesisTmpDB, logger)
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

		sd, err := dbstate.NewSharedDomains(tx, logger)
		if err != nil {
			return err
		}
		defer sd.Close()

		blockNum := uint64(0)
		txNum := uint64(1) //2 system txs in begin/end of block. Attribute state-writes to first, consensus state-changes to second

		//r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)
		r, w := state.NewReaderV3(sd.AsGetter(tx)), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
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

		addrs := sortedAllocAddresses(g.Alloc)
		for _, addr := range addrs {
			account := g.Alloc[addr]

			balance, overflow := uint256.FromBig(account.Balance)
			if overflow {
				panic("overflow at genesis allocs")
			}
			statedb.AddBalance(addr, *balance, tracing.BalanceIncreaseGenesisBalance)
			statedb.SetCode(addr, account.Code)
			statedb.SetNonce(addr, account.Nonce)
			var slotVal uint256.Int
			for key, value := range account.Storage {
				slotVal.SetBytes(value.Bytes())
				statedb.SetState(addr, key, slotVal)
			}

			if len(account.Constructor) > 0 {
				if _, err = core.SysCreate(addr, account.Constructor, g.Config, statedb, head); err != nil {
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

		rh, err := sd.ComputeCommitment(context.Background(), true, blockNum, txNum, "genesis")
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

// GenesisWithoutStateToBlock creates the genesis block, assuming an empty state.
func GenesisWithoutStateToBlock(g *types.Genesis) (head *types.Header, withdrawals []*types.Withdrawal) {
	head = &types.Header{
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
		Root:          empty.RootHash,
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

	withdrawals = nil
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

	// these fields need to be overriden for Bor running in a kurtosis devnet
	if g.Config != nil && g.Config.Bor != nil && g.Config.ChainID.Uint64() == polygonchain.BorKurtosisDevnetChainId {
		withdrawals = []*types.Withdrawal{}
		head.BlobGasUsed = new(uint64)
		head.ExcessBlobGas = new(uint64)
		emptyHash := common.HexToHash("0x0")
		head.ParentBeaconBlockRoot = &emptyHash
	}
	return
}

func sortedAllocAddresses(m types.GenesisAlloc) []common.Address {
	addrs := make([]common.Address, 0, len(m))
	for addr := range m {
		addrs = append(addrs, addr)
	}

	sort.Slice(addrs, func(i, j int) bool {
		return bytes.Compare(addrs[i][:], addrs[j][:]) < 0
	})
	return addrs
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
