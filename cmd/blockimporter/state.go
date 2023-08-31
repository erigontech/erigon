package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	coreState "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/log/v3"
)

type State struct {
	db              *DB
	blockNum        *big.Int
	totalDifficulty *big.Int
	chainConfig     *chain.Config
}

// Account that is used to perform withdraws/deposits
const chainID int64 = 355113

func NewState(db *DB, initialBalances []BalanceEntry) (*State, error) {
	tx, err := db.GetChain().BeginRw(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	state := &State{db: db}

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
		_, _, err := core.CommitGenesisBlock(db.GetChain(), &genesis, "")
		if err != nil {
			return nil, err
		}

		state.blockNum = big.NewInt(1)
		state.totalDifficulty = big.NewInt(0)
		state.chainConfig = &chainConfig
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

		tx.Commit()
	}

	return state, err
}

func (state *State) ProcessBlock(block types.Block) error {
	tx, err := state.db.GetChain().BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()

	batch := olddb.NewHashBatch(tx, make(<-chan struct{}), ".")
	defer batch.Rollback()
	stateReader := coreState.NewPlainStateReader(tx)
	stateWriter := coreState.NewPlainStateWriter(tx, tx, block.NumberU64())
	getTracer := func(txIndex int, txHash common.Hash) (vm.EVMLogger, error) {
		return nil, nil
	}
	callTracer := calltracer.NewCallTracer()
	vmConfig := vm.Config{
		Tracer: callTracer,
		Debug:  true,
	}
	getHashFn := core.GetHashFn(block.Header(), nil)
	engine := ethash.NewFaker()
	execRs, err := core.ExecuteBlockEphemerally(state.chainConfig, &vmConfig, getHashFn, engine, &block,
		stateReader, stateWriter, stagedsync.NewEpochReader(tx),
		stagedsync.NewChainReaderImpl(state.chainConfig, tx, nil),
		getTracer)
	if err != nil {
		// NOTE: Temporary fix
		log.Error(fmt.Sprintf("failed to process block %d: %s, ignoring it", block.NumberU64(), err))
		state.blockNum.Add(state.blockNum, big.NewInt(1))

		return nil
	}

	stateSyncReceipt := execRs.StateSyncReceipt
	if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusFailed {
		return fmt.Errorf("block execution failed")
	}
	if len(execRs.Rejected) != 0 {
		return fmt.Errorf("some of the transactions were rejected")
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

	log.Info(fmt.Sprintf("processed block %d", block.NumberU64()))
	return tx.Commit()
}

func (state *State) BlockNum() uint64 {
	return state.blockNum.Uint64()
}
