package main

import (
	"context"
	"math/big"
	"os"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"

	"github.com/davecgh/go-spew/spew"
)

func main() {
	logger := newLogger()

	db, err := newDatabase("./db", logger, kv.ChainDB)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	db_pool, err := newDatabase("./db", logger, kv.TxPoolDB)
	if err != nil {
		panic(err)
	}
	defer db_pool.Close()

	db_consensus, err := newDatabase("./db", logger, kv.ConsensusDB)
	if err != nil {
		panic(err)
	}
	defer db_consensus.Close()

	// Create genesis block
	genesis := core.Genesis{}
	initState := make(core.GenesisAlloc)
	initState[common.HexToAddress("0xb0e5863d0ddf7e105e409fee0ecc0123a362e14b")] = core.GenesisAccount{
		Balance: max_balance(),
	}
	genesis.Alloc = initState
	chainConfig := chain.Config{
		ChainID: big.NewInt(355113),
	}
	genesis.Config = &chainConfig
	core.MustCommitGenesisBlock(db, &genesis)

	for blockNum, path := range os.Args[1:] {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}

		var block types.Block
		if err = block.DecodeRLP(rlp.NewStream(file, 0)); err != nil {
			panic(err)
		}

		spew.Dump(block)

		tx, err := db.BeginRw(context.Background())
		if err != nil {
			panic(err)
		}

		batch := olddb.NewHashBatch(tx, make(<-chan struct{}), ".")
		stateReader := state.NewPlainStateReader(batch)
		stateWriter := state.NewPlainStateWriter(batch, tx, block.NumberU64())

		// where the magic happens

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

		execRs, err := core.ExecuteBlockEphemerally(&chainConfig, &vmConfig, getHashFn, engine, &block, stateReader, stateWriter, stagedsync.NewEpochReader(tx), stagedsync.NewChainReaderImpl(&chainConfig, tx, nil), getTracer)
		if err != nil {
			panic(err)
		}

		if err = rawdb.WriteHeaderNumber(batch, block.Hash(), block.NumberU64()); err != nil {
			panic(err)
		}

		if err = rawdb.WriteCanonicalHash(batch, block.Hash(), block.NumberU64()); err != nil {
			panic(err)
		}

		if err := rawdb.WriteHeadHeaderHash(batch, block.Hash()); err != nil {
			panic(err)
		}

		if err = batch.Commit(); err != nil {
			panic(err)
		}

		if err = rawdb.WriteBlock(tx, &block); err != nil {
			panic(err)
		}

		receipts := execRs.Receipts
		stateSyncReceipt := execRs.StateSyncReceipt

		if err = rawdb.AppendReceipts(tx, (uint64)(blockNum+1), receipts); err != nil {
			panic(err)
		}

		if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusSuccessful {
			if err := rawdb.WriteBorReceipt(tx, block.Hash(), block.NumberU64(), stateSyncReceipt); err != nil {
				panic(err)
			}
		}

		if err = tx.Commit(); err != nil {
			panic(err)
		}
	}
}

func max_balance() *big.Int {
	var bytes [32]uint8
	for i, _ := range bytes {
		bytes[i] = 255
	}

	val := &big.Int{}
	val.SetBytes(bytes[:])

	return val
}

func newLogger() log.Logger {
	return logging.GetLogger("")
}

func newDatabase(path string, logger log.Logger, label kv.Label) (kv.RwDB, error) {
	config := nodecfg.DefaultConfig
	config.Dirs.DataDir = path
	return node.OpenDatabase(&config, logger, label)
}
