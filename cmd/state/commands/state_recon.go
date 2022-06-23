package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/RoaringBitmap/roaring/roaring64"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(reconCmd)
	withDataDir(reconCmd)
	rootCmd.AddCommand(reconCmd)
}

var reconCmd = &cobra.Command{
	Use:   "recon",
	Short: "Exerimental command to reconstitute the state from state history at given block",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Recon(genesis, logger)
	},
}

func Recon(genesis *core.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	ctx := context.Background()
	aggPath := filepath.Join(datadir, "erigon22")
	agg, err := libstate.NewAggregator(aggPath, AggregationStep)
	if err != nil {
		return fmt.Errorf("create history: %w", err)
	}
	defer agg.Close()
	reconDbPath := path.Join(datadir, "recondb")
	if _, err = os.Stat(reconDbPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err = os.RemoveAll(reconDbPath); err != nil {
		return err
	}
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).WriteMap().Open()
	if err != nil {
		return err
	}
	var rwTx kv.RwTx
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	agg.SetTx(rwTx)
	var blockReader services.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.Reopen(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	// Compute mapping blockNum -> last TxNum in that block
	txNums := make([]uint64, allSnapshots.BlocksAvailable()+1)
	if err = allSnapshots.Bodies.View(func(bs []*snapshotsync.BodySegment) error {
		for _, b := range bs {
			if err = b.Iterate(func(blockNum, baseTxNum, txAmount uint64) {
				txNums[blockNum] = baseTxNum + txAmount
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("build txNum => blockNum mapping: %w", err)
	}
	fmt.Printf("txNums = %d\n", txNums[:20])
	endTxNumMinimax := agg.EndTxNumMinimax()
	fmt.Printf("Max txNum in files: %d\n", endTxNumMinimax)
	blockNum := uint64(sort.Search(len(txNums), func(i int) bool {
		return txNums[i] > endTxNumMinimax
	}))
	if blockNum == uint64(len(txNums)) {
		return fmt.Errorf("mininmax txNum not found in snapshot blocks: %d", endTxNumMinimax)
	}
	if blockNum == 0 {
		return fmt.Errorf("not enough transactions in the history data")
	}
	if block+1 > blockNum {
		return fmt.Errorf("specified block %d which is higher than available %d", block, blockNum)
	}
	blockNum = block + 1
	txNum := txNums[blockNum-1]
	fmt.Printf("Corresponding block num = %d, txNum = %d\n", blockNum, txNum)
	bitmap := agg.ReconBitmap(txNum)
	fmt.Printf("Bitmap length = %d\n", bitmap.GetCardinality())
	firstBlock := true
	var lastBlockNum uint64
	var lastBlockHash common.Hash
	var lastHeader *types.Header
	var lastSigner *types.Signer
	var lastRules *params.Rules
	var doneBitmap roaring64.Bitmap
	stateReader := state.NewHistoryReaderNoState(agg, &doneBitmap)
	noop := state.NewNoopWriter()
	stateWriter := state.NewStateReconWriter(agg)
	stateReader.SetTx(rwTx)
	stateWriter.SetTx(rwTx)
	it := bitmap.Iterator()
	count := 0
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, err := blockReader.Header(ctx, nil, hash, number)
		if err != nil {
			panic(err)
		}
		return h
	}
	engine := initConsensusEngine(chainConfig, logger, allSnapshots)
	for it.HasNext() {
		txNum := uint64(it.Next())
		agg.SetTxNum(txNum)
		stateReader.SetTxNum(txNum)
		stateWriter.SetTxNum(txNum)
		// Find block number
		blockNum := uint64(sort.Search(len(txNums), func(i int) bool {
			return txNums[i] > txNum
		}))
		if firstBlock || blockNum > lastBlockNum {
			if lastHeader, err = blockReader.HeaderByNumber(ctx, nil, blockNum); err != nil {
				return err
			}
			lastBlockNum = blockNum
			lastBlockHash = lastHeader.Hash()
			lastSigner = types.MakeSigner(chainConfig, blockNum)
			lastRules = chainConfig.Rules(blockNum)
			firstBlock = false
		}
		var startTxNum uint64
		if blockNum > 0 {
			startTxNum = txNums[blockNum-1]
		}
		ibs := state.New(stateReader)
		daoForkTx := chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Uint64() == blockNum && txNum == txNums[blockNum-1]
		if blockNum == 0 {
			fmt.Printf("txNum=%d, blockNum=%d, Genesis\n", txNum, blockNum)
			// Genesis block
			_, ibs, err = genesis.ToBlock()
			if err != nil {
				return err
			}
		} else if daoForkTx {
			fmt.Printf("txNum=%d, blockNum=%d, DAO fork\n", txNum, blockNum)
			misc.ApplyDAOHardFork(ibs)
			if err := ibs.FinalizeTx(lastRules, noop); err != nil {
				return err
			}
		} else if txNum+1 == txNums[blockNum] {
			fmt.Printf("txNum=%d, blockNum=%d, finalisation of the block\n", txNum, blockNum)
			// End of block transaction in a block
			block, _, err := blockReader.BlockWithSenders(ctx, nil, lastBlockHash, blockNum)
			if err != nil {
				return err
			}
			if _, _, err := engine.Finalize(chainConfig, lastHeader, ibs, block.Transactions(), block.Uncles(), nil /* receipts */, nil, nil, nil); err != nil {
				return fmt.Errorf("finalize of block %d failed: %w", blockNum, err)
			}
		} else {
			txIndex := txNum - startTxNum - 1
			fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
			txn, err := blockReader.TxnByIdxInBlock(ctx, nil, blockNum, int(txIndex))
			if err != nil {
				return err
			}
			txHash := txn.Hash()
			msg, err := txn.AsMessage(*lastSigner, lastHeader.BaseFee, lastRules)
			if err != nil {
				return err
			}
			gp := new(core.GasPool).AddGas(msg.Gas())
			usedGas := new(uint64)
			vmConfig := vm.Config{NoReceipts: true, SkipAnalysis: core.SkipAnalysis(chainConfig, blockNum)}
			contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
			ibs.Prepare(txHash, lastBlockHash, int(txIndex))
			_, _, err = core.ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, noop, lastHeader, txn, usedGas, vmConfig, contractHasTEVM)
			if err != nil {
				return fmt.Errorf("could not apply tx %d [%x] failed: %w", txIndex, txHash, err)
			}
		}
		if err = ibs.CommitBlock(lastRules, stateWriter); err != nil {
			return err
		}
		if err := agg.FinishTx(); err != nil {
			return fmt.Errorf("finish tx failed: %w", err)
		}
		doneBitmap.Add(txNum)
		count++
		commit := !it.HasNext()
		if count%1_000_000 == 0 {
			log.Info("Processed", "transactions", count)
			var spaceDirty uint64
			if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
				return fmt.Errorf("retrieving spaceDirty: %w", err)
			}
			if spaceDirty >= dirtySpaceThreshold {
				log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
				commit = true
			}
		}
		if commit {
			if err = rwTx.Commit(); err != nil {
				return err
			}
			if rwTx, err = db.BeginRw(ctx); err != nil {
				return err
			}
			agg.SetTx(rwTx)
			stateReader.SetTx(rwTx)
			stateWriter.SetTx(rwTx)
		}
	}
	log.Info("Completed tx replay phase")
	count = 0
	accountsIt := agg.IterateAccountsHistory(txNum)
	for accountsIt.HasNext() {
		key, val := accountsIt.Next()
		if len(val) > 0 {
			var a accounts.Account
			a.Reset()
			pos := 0
			nonceBytes := int(val[pos])
			pos++
			if nonceBytes > 0 {
				a.Nonce = bytesToUint64(val[pos : pos+nonceBytes])
				pos += nonceBytes
			}
			balanceBytes := int(val[pos])
			pos++
			if balanceBytes > 0 {
				a.Balance.SetBytes(val[pos : pos+balanceBytes])
				pos += balanceBytes
			}
			codeHashBytes := int(val[pos])
			pos++
			if codeHashBytes > 0 {
				copy(a.CodeHash[:], val[pos:pos+codeHashBytes])
				pos += codeHashBytes
			}
			if pos >= len(val) {
				fmt.Printf("panic ReadAccountData(%x)=>[%x]\n", key, val)
			}
			incBytes := int(val[pos])
			pos++
			if incBytes > 0 {
				a.Incarnation = bytesToUint64(val[pos : pos+incBytes])
			}
			value := make([]byte, a.EncodingLengthForStorage())
			a.EncodeForStorage(value)
			if err = rwTx.Put(kv.PlainState, key, value); err != nil {
				return err
			}
			fmt.Printf("Account [%x]=>[%+v]\n", key, &a)
			count++
			if count%1_000_000 == 0 {
				log.Info("Processed", "accounts", count)
				var spaceDirty uint64
				if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
					return fmt.Errorf("retrieving spaceDirty: %w", err)
				}
				if spaceDirty >= dirtySpaceThreshold {
					log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
					if err = rwTx.Commit(); err != nil {
						return err
					}
					if rwTx, err = db.BeginRw(ctx); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	count = 0
	storageIt := agg.IterateStorageHistory(txNum)
	for storageIt.HasNext() {
		key, val := storageIt.Next()
		if len(val) > 0 {
			compositeKey := dbutils.PlainGenerateCompositeStorageKey(key[:20], state.FirstContractIncarnation, key[20:])
			if err = rwTx.Put(kv.PlainState, compositeKey, val); err != nil {
				return err
			}
			fmt.Printf("Storage [%x] => [%x]\n", compositeKey, val)
			count++
			if count%1_000_000 == 0 {
				log.Info("Processed", "storage", count)
				var spaceDirty uint64
				if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
					return fmt.Errorf("retrieving spaceDirty: %w", err)
				}
				if spaceDirty >= dirtySpaceThreshold {
					log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
					if err = rwTx.Commit(); err != nil {
						return err
					}
					if rwTx, err = db.BeginRw(ctx); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	count = 0
	codeIt := agg.IterateCodeHistory(txNum)
	for codeIt.HasNext() {
		key, val := codeIt.Next()
		if len(val) > 0 {
			codeHash := crypto.Keccak256(val)
			if err = rwTx.Put(kv.Code, codeHash[:], val); err != nil {
				return err
			}
			compositeKey := dbutils.PlainGenerateStoragePrefix(key, state.FirstContractIncarnation)
			if err = rwTx.Put(kv.PlainContractCode, compositeKey, codeHash[:]); err != nil {
				return err
			}
			fmt.Printf("Code [%x] => [%x]\n", compositeKey, val)
			count++
			if count%1_000_000 == 0 {
				log.Info("Processed", "code", count)
				var spaceDirty uint64
				if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
					return fmt.Errorf("retrieving spaceDirty: %w", err)
				}
				if spaceDirty >= dirtySpaceThreshold {
					log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
					if err = rwTx.Commit(); err != nil {
						return err
					}
					if rwTx, err = db.BeginRw(ctx); err != nil {
						return err
					}
				}
			}
		}
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	log.Info("Computing hashed state")
	tmpDir := filepath.Join(datadir, "tmp")
	if err = stagedsync.PromoteHashedStateCleanly("recon", rwTx, stagedsync.StageHashStateCfg(nil, tmpDir), make(chan struct{}, 1)); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	if _, err = stagedsync.RegenerateIntermediateHashes("recon", rwTx, stagedsync.StageTrieCfg(nil, false /* checkRoot */, false /* saveHashesToDB */, tmpDir, blockReader), common.Hash{}, make(chan struct{}, 1)); err != nil {
		return err
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	return nil
}
