package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	coreState "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/log/v3"

	datadir2 "github.com/ledgerwatch/erigon-lib/common/datadir"
)

type State struct {
	db              *DB
	trieLoader      *trie.FlatDBTrieLoader
	blockNum        *big.Int
	totalDifficulty *big.Int
	chainConfig     *chain.Config
}

func NewState(db *DB, initialBalances []BalanceEntry, chainID int64) (*State, error) {
	tx, err := db.GetChain().BeginRw(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	state := &State{
		db:         db,
		trieLoader: trie.NewFlatDBTrieLoader("loader"),
	}

	if err := state.trieLoader.Reset(trie.NewRetainList(0), nil, nil, false); err != nil {
		return nil, err
	}

	if blockNum := rawdb.ReadCurrentBlockNumber(tx); blockNum == nil || *blockNum == 0 {
		// Close the transaction
		tx.Rollback()

		// Add genesis block
		genesis := core.Genesis{}
		initState := make(core.GenesisAlloc)
		for _, balanceRecord := range initialBalances {
			initState[balanceRecord.Address] = core.GenesisAccount{
				Balance: &balanceRecord.Balance,
			}
		}
		genesis.Alloc = initState
		chainConfig := chain.Config{
			ChainID: big.NewInt(chainID),

			// Set the chain spec corresponding to the latest one from revm
			HomesteadBlock:        big.NewInt(0),
			DAOForkBlock:          big.NewInt(0),
			DAOForkSupport:        true,
			TangerineWhistleBlock: big.NewInt(0),
			SpuriousDragonBlock:   big.NewInt(0),
			ByzantiumBlock:        big.NewInt(0),
			ConstantinopleBlock:   big.NewInt(0),
			PetersburgBlock:       big.NewInt(0),
			IstanbulBlock:         big.NewInt(0),
			MuirGlacierBlock:      big.NewInt(0),
			BerlinBlock:           big.NewInt(0),
			LondonBlock:           big.NewInt(0),
			ArrowGlacierBlock:     big.NewInt(0),
			GrayGlacierBlock:      big.NewInt(0),
			MergeNetsplitBlock:    big.NewInt(0),
		}
		genesis.Config = &chainConfig
		_, block, err := core.CommitGenesisBlock(db.GetChain(), &genesis, "")
		if err != nil {
			return nil, err
		}

		state.blockNum = big.NewInt(1)
		state.totalDifficulty = big.NewInt(0)
		state.chainConfig = &chainConfig

		// cInitialize data needed for state root calculation
		tx, err := db.GetChain().BeginRw(context.Background())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()

		dirs := datadir2.New(db.path)
		if err = stagedsync.PromoteHashedStateCleanly("logPrefix", tx, stagedsync.StageHashStateCfg(db.chain, dirs, false, nil), context.Background()); err != nil {
			return nil, fmt.Errorf("error while promoting state: %v", err)
		}

		root, err := state.trieLoader.CalcTrieRoot(tx, nil, nil)
		if err != nil {
			return nil, err
		} else if root != block.Root() {
			// This error may happen if we forgot to initialize the data for state root calculation.
			// Better fail here in this case rather than in the first block
			return nil, fmt.Errorf("invalid root, have: %s, want: %s", root.String(), block.Root().String())
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
	} else {
		state.blockNum = (&big.Int{}).SetUint64(*blockNum + 1)

		currentHash := rawdb.ReadHeadBlockHash(tx)
		if state.totalDifficulty, err = rawdb.ReadTdByHash(tx, currentHash); err != nil {
			return nil, err
		}

		genesisBlock, err := rawdb.ReadBlockByNumber(tx, 0)
		if err != nil {
			return nil, err
		}

		if state.chainConfig, err = rawdb.ReadChainConfig(tx, genesisBlock.Hash()); err != nil {
			return nil, err
		}

		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}

	return state, err
}

func (state *State) ProcessBlocks(blocks []types.Block, saveHistoryData bool) error {
	if len(blocks) == 0 {
		return nil
	}

	log.Info(fmt.Sprintf("processing %d blocks", len(blocks)))

	tx, err := state.db.GetChain().BeginRw(context.Background())
	if err != nil {
		return err
	}

	for _, block := range blocks {
		if err := state.processBlock(block, tx, saveHistoryData); err != nil {
			return err
		}
	}

	log.Info("verifying state root")
	prevBlockNum := blocks[0].NumberU64() - 1
	currBlock := blocks[len(blocks)-1]
	currBlockNum := currBlock.NumberU64()

	dirs := datadir2.New(state.db.path)
	if err = stagedsync.PromoteHashedStateIncrementally("hashedstate", prevBlockNum, currBlockNum, tx, stagedsync.StageHashStateCfg(nil, dirs, false, nil), context.Background(), false); err != nil {
		return err
	}

	s := stagedsync.StageState{
		BlockNumber: prevBlockNum,
	}
	cfg := stagedsync.StageTrieCfg(state.db.chain, false, true, true, state.db.path, nil, nil, false, nil)
	if root, err := stagedsync.IncrementIntermediateHashes("increment hashes", &s, tx, currBlockNum, cfg, common.Hash{}, nil); err != nil {
		return err
	} else if root != currBlock.Root() {
		return fmt.Errorf("invalid root, have: %s, want: %s", root.String(), currBlock.Root().String())
	}

	log.Info("state root verified")

	err = tx.Commit()

	log.Info(fmt.Sprintf("%d blocks successfully processed", len(blocks)))

	return err
}

func (state *State) processBlock(block types.Block, tx kv.RwTx, saveHistoryData bool) error {
	log.Info(fmt.Sprintf("processing block %d", block.NumberU64()))

	batch := olddb.NewHashBatch(tx, make(<-chan struct{}), ".")
	defer batch.Rollback()
	stateReader := coreState.NewPlainStateReader(batch)
	stateWriter := coreState.NewPlainStateWriter(batch, tx, block.NumberU64())
	getTracer := func(txIndex int, txHash common.Hash) (vm.EVMLogger, error) {
		return nil, nil
	}
	callTracer := calltracer.NewCallTracer()
	vmConfig := vm.Config{
		Tracer: callTracer,
		Debug:  true,
	}
	getHashFn := core.GetHashFn(block.Header(), nil)
	engine := newFakeConsensus()
	execRs, err := core.ExecuteBlockEphemerally(state.chainConfig, &vmConfig, getHashFn, engine, &block,
		stateReader, stateWriter, stagedsync.NewEpochReader(tx),
		stagedsync.NewChainReaderImpl(state.chainConfig, tx, nil),
		getTracer)
	if err != nil {
		log.Error(fmt.Sprintf("failed to process block %d: %s", block.NumberU64(), err))

		return err
	}

	stateSyncReceipt := execRs.StateSyncReceipt
	if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusFailed {
		return fmt.Errorf("block execution failed")
	}
	if len(execRs.Rejected) != 0 {
		return fmt.Errorf("some of the transactions were rejected")
	}

	if saveHistoryData {
		if err := stateWriter.WriteHistory(); err != nil {
			return fmt.Errorf("failed to write history: %v", err)
		}
	}
	if err := rawdb.WriteHeaderNumber(batch, block.Hash(), block.NumberU64()); err != nil {
		return err
	}
	if err := rawdb.WriteCanonicalHash(batch, block.Hash(), block.NumberU64()); err != nil {
		return err
	}
	if err := rawdb.WriteHeadHeaderHash(batch, block.Hash()); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return err
	}

	if err := rawdb.WriteBlock(tx, &block); err != nil {
		return err
	}

	receipts := execRs.Receipts
	if err := rawdb.AppendReceipts(tx, (uint64)(state.blockNum.Uint64()+1), receipts); err != nil {
		return err
	}
	if stateSyncReceipt != nil {
		if err := rawdb.WriteBorReceipt(tx, block.Hash(), block.NumberU64(), stateSyncReceipt); err != nil {
			return err
		}
	}

	if err := stages.SaveStageProgress(tx, stages.Execution, block.NumberU64()); err != nil {
		return err
	}

	signer := types.MakeSigner(state.chainConfig, block.NumberU64())
	for tx_index, tx := range block.Transactions() {
		if sender, err := tx.Sender(*signer); err == nil {
			block.Transactions()[tx_index].SetSender(sender)
		} else {
			return err
		}
	}
	if err := rawdb.WriteSenders(tx, block.Hash(), block.NumberU64(), block.Body().SendersFromTxs()); err != nil {
		return err
	}

	state.totalDifficulty.Add(state.totalDifficulty, block.Difficulty())
	if err := rawdb.WriteTd(tx, block.Hash(), block.NumberU64(), state.totalDifficulty); err != nil {
		return err
	}

	// NOTE: it's absolutely inconsistent that `WriteTxLookupEntries` doesn't return err
	rawdb.WriteTxLookupEntries(tx, &block)

	state.blockNum.Add(state.blockNum, big.NewInt(1))

	if err := callTracer.WriteToDb(tx, &block, vmConfig); err != nil {
		return err
	}

	tmpDir, err := ioutil.TempDir("", fmt.Sprintf("block_%d_traces", block.NumberU64()))
	if err != nil {
		return fmt.Errorf("failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	if err := stagedsync.ProcessCallTracesForBlock(tx, block.NumberU64(), tmpDir); err != nil {
		return err
	}

	return nil
}

func (state *State) BlockNum() uint64 {
	return state.blockNum.Uint64()
}

type FakeConsensus struct {
	ethash.FakeEthash
}

func newFakeConsensus() *FakeConsensus {
	return &FakeConsensus{
		FakeEthash: *ethash.NewFaker(),
	}
}

// Override base method not to accumulate rewards to coinbase
func (c *FakeConsensus) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	e consensus.EpochReader, chain consensus.ChainHeaderReader, syscall consensus.SystemCall,
) (types.Transactions, types.Receipts, error) {
	return txs, r, nil
}
