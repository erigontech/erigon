package txpool

import (
	"context"
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

	for _, limboBatch := range limboBatchDetails {
		for _, limboTx := range limboBatch.Transactions {
			if !limboTx.hasRoot() {
				return
			}
		}
	}

	tx, err := _this.db.BeginRo(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()

	// we just need some counter variable with large used values in order verify not to complain
	batchCounters := vm.NewBatchCounterCollector(256, 1, _this.zkCfg.VirtualCountersSmtReduction, true)
	unlimitedCounters := batchCounters.NewCounters().UsedAsMap()
	for k := range unlimitedCounters {
		unlimitedCounters[k] = math.MaxInt32
	}

	invalidTxs := []*string{}

	for _, limboBatch := range limboBatchDetails {
		for _, limboTx := range limboBatch.Transactions {
			request := legacy_executor_verifier.NewVerifierRequest(limboBatch.BatchNumber, limboBatch.ForkId, limboTx.Root, unlimitedCounters)
			err := _this.verifier.VerifySync(tx, request, limboBatch.Witness, limboTx.StreamBytes, limboBatch.TimestampLimit, limboBatch.FirstBlockNumber, limboBatch.L1InfoTreeMinTimestamps)
			if err != nil {
				idHash := hexutils.BytesToHex(limboTx.Hash[:])
				invalidTxs = append(invalidTxs, &idHash)
				log.Info("[Limbo pool processor]", "invalid tx", limboTx.Hash, "err", err)
				continue
			}

			log.Info("[Limbo pool processor]", "valid tx", limboTx.Hash)
		}
	}

	_this.txPool.MarkProcessedLimboDetails(size, invalidTxs)

}
