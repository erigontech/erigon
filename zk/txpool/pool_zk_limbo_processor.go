package txpool

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	"github.com/ledgerwatch/log/v3"
	"github.com/status-im/keycard-go/hexutils"
)

type LimboSubPoolProcessor struct {
	zkCfg       *ethconfig.Zk
	chainConfig *chain.Config
	db          kv.RwDB
	txPool      *TxPool
	verifier    *legacy_executor_verifier.LegacyExecutorVerifier
	quit        <-chan struct{}
}

func NewLimboSubPoolProcessor(ctx context.Context, zkCfg *ethconfig.Zk, chainConfig *chain.Config, db kv.RwDB, txPool *TxPool, verifier *legacy_executor_verifier.LegacyExecutorVerifier) *LimboSubPoolProcessor {
	return &LimboSubPoolProcessor{
		zkCfg:       zkCfg,
		chainConfig: chainConfig,
		db:          db,
		txPool:      txPool,
		verifier:    verifier,
		quit:        ctx.Done(),
	}
}

func (_this *LimboSubPoolProcessor) StartWork() {
	go func() {
		tick := time.NewTicker(30 * time.Second)
		defer tick.Stop()
	LOOP:
		for {
			select {
			case <-_this.quit:
				break LOOP
			case <-tick.C:
				_this.run()
			}
		}
	}()
}

func (_this *LimboSubPoolProcessor) run() {
	log.Info("[Limbo pool processor] Starting")
	defer log.Info("[Limbo pool processor] End")

	ctx := context.Background()
	limboBatchDetails := _this.txPool.GetLimboDetailsCloned()

	size := len(limboBatchDetails)
	if size == 0 {
		return
	}

	totalTransactions := 0
	processedTransactions := 0
	for _, limboBatch := range limboBatchDetails {
		for _, limboBlock := range limboBatch.Blocks {
			for _, limboTx := range limboBlock.Transactions {
				if !limboTx.hasRoot() {
					return
				}
				totalTransactions++
			}
		}
	}

	tx, err := _this.db.BeginRo(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()

	// we just need some counter variable with large used values in order verify not to complain
	batchCounters := vm.NewBatchCounterCollector(256, 1, _this.zkCfg.VirtualCountersSmtReduction, true, nil)
	unlimitedCounters := batchCounters.NewCounters().UsedAsMap()
	for mapKey := range unlimitedCounters {
		unlimitedCounters[mapKey] = math.MaxInt32
	}

	invalidTxs := []*string{}

	for i, limboBatch := range limboBatchDetails {
		blockNumbers := make([]uint64, 0, len(limboBatch.Blocks))
		for _, limboBlock := range limboBatch.Blocks {
			blockNumbers = append(blockNumbers, limboBlock.BlockNumber)
			for _, limboTx := range limboBlock.Transactions {
				request := legacy_executor_verifier.NewVerifierRequest(limboBatch.ForkId, limboBatch.BatchNumber, blockNumbers, limboTx.Root, unlimitedCounters)
				err := _this.verifier.VerifySync(tx, request, limboBatch.Witness, limboTx.StreamBytes, limboBlock.Timestamp, limboBatch.L1InfoTreeMinTimestamps)
				if err != nil {
					idHash := hexutils.BytesToHex(limboTx.Hash[:])
					invalidTxs = append(invalidTxs, &idHash)
					log.Info("[Limbo pool processor]", "invalid tx", limboTx.Hash, "err", err)
					continue
				}

				processedTransactions++
				log.Info("[Limbo pool processor]", "valid tx", limboTx.Hash, "progress", fmt.Sprintf("transactions: %d of %d, batches: %d of %d", processedTransactions, totalTransactions, i+1, len(limboBatchDetails)))
			}
		}
	}

	_this.txPool.MarkProcessedLimboDetails(size, invalidTxs)
}
