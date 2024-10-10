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
	"context"
	"embed"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"sync"

	"github.com/c2h5oh/datasize"
	erigonchain "github.com/gateway-fm/cdk-erigon-lib/chain"
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/mdbx"
	"github.com/gateway-fm/cdk-erigon-lib/kv/rawdbv3"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/log/v3"

	"os"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/params/networkname"
	eridb "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"golang.org/x/exp/slices"
)

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
func CommitGenesisBlock(db kv.RwDB, genesis *types.Genesis, tmpDir string) (*chain.Config, *types.Block, error) {
	return CommitGenesisBlockWithOverride(db, genesis, nil, tmpDir)
}

func CommitGenesisBlockWithOverride(db kv.RwDB, genesis *types.Genesis, overrideShanghaiTime *big.Int, tmpDir string) (*chain.Config, *types.Block, error) {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	c, b, err := WriteGenesisBlock(tx, genesis, overrideShanghaiTime, tmpDir)
	if err != nil {
		return c, b, err
	}
	err = tx.Commit()
	if err != nil {
		return c, b, err
	}
	return c, b, nil
}

func WriteGenesisBlock(tx kv.RwTx, genesis *types.Genesis, overrideShanghaiTime *big.Int, tmpDir string) (*chain.Config, *types.Block, error) {
	if genesis != nil && genesis.Config == nil {
		return params.AllProtocolChanges, nil, types.ErrGenesisNoConfig
	}
	// Just commit the new block if there is no stored genesis block.
	storedHash, storedErr := rawdb.ReadCanonicalHash(tx, 0)
	if storedErr != nil {
		return nil, nil, storedErr
	}

	applyOverrides := func(config *chain.Config) {
		if overrideShanghaiTime != nil {
			config.ShanghaiTime = overrideShanghaiTime
		}
	}

	if (storedHash == libcommon.Hash{}) {
		custom := true
		if genesis == nil {
			log.Info("Writing main-net genesis block")
			genesis = MainnetGenesisBlock()
			custom = false
		}
		// clear tmpDir
		if tmpDir != "" {
			if err := os.RemoveAll(tmpDir); err != nil {
				return genesis.Config, nil, err
			}
			if err := os.MkdirAll(tmpDir, 0755); err != nil {
				return genesis.Config, nil, err
			}
		}
		applyOverrides(genesis.Config)
		block, _, _, err1 := write(tx, genesis, tmpDir)
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
		block, _, _, err1 := GenesisToBlock(genesis, tmpDir)
		if err1 != nil {
			return genesis.Config, nil, err1
		}
		hash := block.Hash()
		if hash != storedHash {
			return genesis.Config, block, &types.GenesisMismatchError{Stored: storedHash, New: hash}
		}
	}
	storedBlock, err := rawdb.ReadBlockByHash(tx, storedHash)
	if err != nil {
		return genesis.Config, nil, err
	}
	// Get the existing chain configuration.
	newCfg := genesis.ConfigOrDefault(storedHash)
	applyOverrides(newCfg)
	if err := newCfg.CheckConfigForkOrder(); err != nil {
		return newCfg, nil, err
	}
	storedCfg, storedErr := rawdb.ReadChainConfig(tx, storedHash)
	if storedErr != nil && newCfg.Bor == nil {
		return newCfg, nil, storedErr
	}
	if storedCfg == nil {
		log.Warn("Found genesis block without chain config")
		err1 := rawdb.WriteChainConfig(tx, storedHash, newCfg)
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

	// set unwanted forks block to max number, so they are not activated
	maxInt := new(big.Int).SetUint64(math.MaxUint64)
	newCfg.LondonBlock = maxInt
	newCfg.ShanghaiTime = maxInt
	newCfg.CancunTime = maxInt
	newCfg.PragueTime = maxInt

	return newCfg, storedBlock, nil
}

func WriteGenesisState(g *types.Genesis, tx kv.RwTx, tmpDir string) (*types.Block, *state.IntraBlockState, *smt.SMT, error) {
	block, statedb, sparseTree, err := GenesisToBlock(g, tmpDir)
	if err != nil {
		return nil, nil, nil, err
	}
	var stateWriter state.StateWriter
	if ethconfig.EnableHistoryV4InTest {
		panic("implement me")
		//tx.(*temporal.Tx).Agg().SetTxNum(0)
		//stateWriter = state.NewWriterV4(tx.(kv.TemporalTx))
		//defer tx.(*temporal.Tx).Agg().StartUnbufferedWrites().FinishWrites()
	} else {
		for addr, account := range g.Alloc {
			if len(account.Code) > 0 || len(account.Storage) > 0 {
				// Special case for weird tests - inaccessible storage
				var b [8]byte
				binary.BigEndian.PutUint64(b[:], state.FirstContractIncarnation)
				if err := tx.Put(kv.IncarnationMap, addr[:], b[:]); err != nil {
					return nil, nil, nil, err
				}
			}
		}
		stateWriter = state.NewPlainStateWriter(tx, tx, 0)
	}

	if block.Number().Sign() != 0 {
		return nil, statedb, sparseTree, fmt.Errorf("can't commit genesis block with number > 0")
	}

	if err := statedb.CommitBlock(&chain.Rules{}, stateWriter); err != nil {
		return nil, statedb, sparseTree, fmt.Errorf("cannot write state: %w", err)
	}
	if csw, ok := stateWriter.(state.WriterWithChangeSets); ok {
		if err := csw.WriteChangeSets(); err != nil {
			return nil, statedb, sparseTree, fmt.Errorf("cannot write change sets: %w", err)
		}
		if err := csw.WriteHistory(); err != nil {
			return nil, statedb, sparseTree, fmt.Errorf("cannot write history: %w", err)
		}
	}
	return block, statedb, sparseTree, nil
}

func MustCommitGenesis(g *types.Genesis, db kv.RwDB, tmpDir string) *types.Block {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	block, _, _, err := write(tx, g, tmpDir)
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
func write(tx kv.RwTx, g *types.Genesis, tmpDir string) (*types.Block, *state.IntraBlockState, *smt.SMT, error) {
	block, statedb, sparseTree, err2 := WriteGenesisState(g, tx, tmpDir)
	if err2 != nil {
		return block, statedb, sparseTree, err2
	}
	config := g.Config
	if config == nil {
		config = params.AllProtocolChanges
	}
	if err := config.CheckConfigForkOrder(); err != nil {
		return nil, nil, nil, err
	}
	if err := rawdb.WriteTd(tx, block.Hash(), block.NumberU64(), g.Difficulty); err != nil {
		return nil, nil, nil, err
	}
	if err := rawdb.WriteBlock(tx, block); err != nil {
		return nil, nil, nil, err
	}
	if err := rawdbv3.TxNums.WriteForGenesis(tx, 1); err != nil {
		return nil, nil, nil, err
	}
	if err := rawdb.WriteReceipts(tx, block.NumberU64(), nil); err != nil {
		return nil, nil, nil, err
	}

	if err := rawdb.WriteCanonicalHash(tx, block.Hash(), block.NumberU64()); err != nil {
		return nil, nil, nil, err
	}

	rawdb.WriteHeadBlockHash(tx, block.Hash())
	if err := rawdb.WriteHeadHeaderHash(tx, block.Hash()); err != nil {
		return nil, nil, nil, err
	}
	if err := rawdb.WriteChainConfig(tx, block.Hash(), config); err != nil {
		return nil, nil, nil, err
	}
	// We support ethash/serenity for issuance (for now)
	if g.Config.Consensus != erigonchain.EtHashConsensus {
		return block, statedb, sparseTree, nil
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
		return nil, nil, nil, err
	}
	return block, statedb, sparseTree, rawdb.WriteTotalBurnt(tx, 0, libcommon.Big0)
}

// GenesisBlockForTesting creates and writes a block in which addr has the given wei balance.
func GenesisBlockForTesting(db kv.RwDB, addr libcommon.Address, balance *big.Int, tmpDir string) *types.Block {
	g := types.Genesis{Alloc: types.GenesisAlloc{addr: {Balance: balance}}, Config: params.TestChainConfig}
	block := MustCommitGenesis(&g, db, tmpDir)
	return block
}

type GenAccount struct {
	Addr    libcommon.Address
	Balance *big.Int
}

func GenesisWithAccounts(db kv.RwDB, accs []GenAccount, tmpDir string) *types.Block {
	g := types.Genesis{Config: params.TestChainConfig}
	allocs := make(map[libcommon.Address]types.GenesisAccount)
	for _, acc := range accs {
		allocs[acc.Addr] = types.GenesisAccount{Balance: acc.Balance}
	}
	g.Alloc = allocs
	block := MustCommitGenesis(&g, db, tmpDir)
	return block
}

// MainnetGenesisBlock returns the Ethereum main net genesis block.
func MainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.MainnetChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa"),
		GasLimit:   5000,
		Difficulty: big.NewInt(17179869184),
		Alloc:      readPrealloc("allocs/mainnet.json"),
	}
}

// SepoliaGenesisBlock returns the Sepolia network genesis block.
func SepoliaGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.SepoliaChainConfig,
		Nonce:      0,
		ExtraData:  []byte("Sepolia, Athens, Attica, Greece!"),
		GasLimit:   30000000,
		Difficulty: big.NewInt(131072),
		Timestamp:  1633267481,
		Alloc:      readPrealloc("allocs/sepolia.json"),
	}
}

// RinkebyGenesisBlock returns the Rinkeby network genesis block.
func RinkebyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.RinkebyChainConfig,
		Timestamp:  1492009146,
		ExtraData:  hexutil.MustDecode("0x52657370656374206d7920617574686f7269746168207e452e436172746d616e42eb768f2244c8811c63729a21a3569731535f067ffc57839b00206d1ad20c69a1981b489f772031b279182d99e65703f0076e4812653aab85fca0f00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   4700000,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/rinkeby.json"),
	}
}

// GoerliGenesisBlock returns the Görli network genesis block.
func GoerliGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.GoerliChainConfig,
		Timestamp:  1548854791,
		ExtraData:  hexutil.MustDecode("0x22466c6578692069732061207468696e6722202d204166726900000000000000e0a2bd4258d2768837baa26a28fe71dc079f84c70000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   10485760,
		Difficulty: big.NewInt(1),
		Alloc:      readPrealloc("allocs/goerli.json"),
	}
}

func MumbaiGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.MumbaiChainConfig,
		Nonce:      0,
		Timestamp:  1558348305,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/mumbai.json"),
	}
}

// BorMainnetGenesisBlock returns the Bor Mainnet network genesis block.
func BorMainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.BorMainnetChainConfig,
		Nonce:      0,
		Timestamp:  1590824836,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_mainnet.json"),
	}
}

func BorDevnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.BorDevnetChainConfig,
		Nonce:      0,
		Timestamp:  1558348305,
		GasLimit:   10000000,
		Difficulty: big.NewInt(1),
		Mixhash:    libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		Coinbase:   libcommon.HexToAddress("0x0000000000000000000000000000000000000000"),
		Alloc:      readPrealloc("allocs/bor_devnet.json"),
	}
}

func GnosisGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     params.GnosisChainConfig,
		Timestamp:  0,
		AuRaSeal:   common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      readPrealloc("allocs/gnosis.json"),
	}
}

func ChiadoGenesisBlock() *types.Genesis {
	return &types.Genesis{
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
var DevnetEtherbase = libcommon.HexToAddress("67b1d87101671b127f5f8714789c7192f7ad340e")

func DeveloperGenesisBlock(period uint64, faucet libcommon.Address) *types.Genesis {
	// Override the default period to the user requested one
	config := *params.AllCliqueProtocolChanges
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

var GenesisTmpDB kv.RwDB
var GenesisDBLock sync.Mutex

// ToBlock creates the genesis block and writes state of a genesis specification
// to the given database (or discards it if nil).
func GenesisToBlock(g *types.Genesis, tmpDir string) (*types.Block, *state.IntraBlockState, *smt.SMT, error) {
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

	// [zkevm] - do not override the gas limit for the genesis block
	//if g.GasLimit == 0 {
	//	head.GasLimit = params.GenesisGasLimit
	//}

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
	sparseDb := eridb.NewMemDb()
	sparseTree := smt.NewSMT(sparseDb, false)
	go func() { // we may run inside write tx, can't open 2nd write tx in same goroutine
		// TODO(yperbasis): use memdb.MemoryMutation instead
		defer wg.Done()
		GenesisDBLock.Lock()
		defer GenesisDBLock.Unlock()
		if GenesisTmpDB == nil {
			GenesisTmpDB = mdbx.NewMDBX(log.New()).InMem(tmpDir).MapSize(2 * datasize.GB).MustOpen()
			defer func() {
				GenesisTmpDB.Close()
				GenesisTmpDB = nil
			}()
		}
		var tx kv.RwTx
		if tx, err = GenesisTmpDB.BeginRw(context.Background()); err != nil {
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

		var ro *big.Int

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

			for k, value := range account.Storage {
				val := uint256.NewInt(0).SetBytes(value.Bytes())
				statedb.SetState(addr, &k, *val)
			}

			if len(account.Constructor) > 0 {
				if _, err = SysCreate(addr, account.Constructor, *g.Config, statedb, head, g.ExcessDataGas); err != nil {
					return
				}
			}

			if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
				statedb.SetIncarnation(addr, state.FirstContractIncarnation)
			}

			ro, err = processAccount(sparseTree, ro, &account, addr)
			if err != nil {
				return
			}
		}
		if err = statedb.FinalizeTx(&chain.Rules{}, w); err != nil {
			return
		}

		root = libcommon.BigToHash(ro)
	}()
	wg.Wait()
	if err != nil {
		return nil, nil, nil, err
	}

	head.Root = root

	return types.NewBlock(head, nil, nil, nil, withdrawals), statedb, sparseTree, nil
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

//go:embed allocs
var allocs embed.FS

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
	case networkname.MainnetChainName:
		return MainnetGenesisBlock()
	case networkname.SepoliaChainName:
		return SepoliaGenesisBlock()
	case networkname.RinkebyChainName:
		return RinkebyGenesisBlock()
	case networkname.GoerliChainName:
		return GoerliGenesisBlock()
	case networkname.MumbaiChainName:
		return MumbaiGenesisBlock()
	case networkname.BorMainnetChainName:
		return BorMainnetGenesisBlock()
	case networkname.BorDevnetChainName:
		return BorDevnetGenesisBlock()
	case networkname.GnosisChainName:
		return GnosisGenesisBlock()
	case networkname.ChiadoChainName:
		return ChiadoGenesisBlock()
	case networkname.HermezMainnetChainName:
		return HermezMainnetGenesisBlock()
	case networkname.HermezMainnetShadowforkChainName:
		return HermezMainnetShadowforkGenesisBlock()
	case networkname.HermezLocalDevnetChainName:
		return HermezLocalDevnetGenesisBlock()
	case networkname.HermezESTestChainName:
		return HermezESTestGenesisBlock()
	case networkname.HermezEtrogChainName:
		return HermezEtrogGenesisBlock()
	case networkname.HermezCardonaChainName:
		return HermezCardonaGenesisBlock()
	case networkname.HermezBaliChainName:
		return HermezBaliGenesisBlock()
	case networkname.XLayerTestnetChainName:
		return XLayerTestnetGenesisBlock()
	case networkname.XLayerMainnetChainName:
		return XLayerMainnetGenesisBlock()
	default:
		return DynamicGenesisBlock(chain)
	}
}
