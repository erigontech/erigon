package main

import (
	"context"
	"math/big"
	"os"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/calltracer"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb/olddb"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
)

func main() {
	logger := newLogger()
	db := newDatabase(".", logger, true)

	// Create genesis block
	genesis := core.Genesis{}
	chainConfig := chain.Config{
		ChainID: big.NewInt(5),
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

		var receipts types.Receipts
		var stateSyncReceipt *types.Receipt
		var execRs *core.EphemeralExecResult
		getHashFn := core.GetHashFn(block.Header(), nil)
		engine := ethash.NewFaker()

		execRs, err = core.ExecuteBlockEphemerally(&chainConfig, &vmConfig, getHashFn, engine, &block, stateReader, stateWriter, stagedsync.NewEpochReader(tx), stagedsync.NewChainReaderImpl(&chainConfig, tx, nil), getTracer)
		if err != nil {
			panic(err)
		}
		receipts = execRs.Receipts
		stateSyncReceipt = execRs.StateSyncReceipt

		if err = rawdb.AppendReceipts(tx, (uint64)(blockNum+1), receipts); err != nil {
			panic(err)
		}

		if stateSyncReceipt != nil && stateSyncReceipt.Status == types.ReceiptStatusSuccessful {
			if err := rawdb.WriteBorReceipt(tx, block.Hash(), block.NumberU64(), stateSyncReceipt); err != nil {
				panic(err)
			}
		}
	}
}

func newLogger() log.Logger {
	return logging.GetLogger("")
}

func newDatabase(path string, logger log.Logger, inMem bool) kv.RwDB {
	opts := mdbx.NewMDBX(logger).Label(kv.ConsensusDB)
	if inMem {
		opts = opts.InMem("")
	} else {
		opts = opts.Path(path)
	}

	return opts.MustOpen()
}
