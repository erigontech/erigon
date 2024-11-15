package vm

import (
	"fmt"
	"math"

	"github.com/ledgerwatch/log/v3"
)

type BatchCounterCollector struct {
	// every new transaction added to the batch affects these counters, so they need their own entry as this will
	// be overwritten time and again as transactions are added
	l2DataCollector         *CounterCollector
	transactions            []*TransactionCounter
	smtLevels               int
	smtLevelsForTransaction int
	blockCount              int
	forkId                  uint16
	unlimitedCounters       bool
	addonCounters           *Counters

	rlpCombinedCounters        Counters
	executionCombinedCounters  Counters
	processingCombinedCounters Counters
}

func NewBatchCounterCollector(smtMaxLevel int, forkId uint16, mcpReduction float64, unlimitedCounters bool, addonCounters *Counters) *BatchCounterCollector {
	smtLevels := calculateSmtLevels(smtMaxLevel, 0, mcpReduction)
	smtLevelsForTransaction := calculateSmtLevels(smtMaxLevel, 32, mcpReduction)
	bcc := BatchCounterCollector{
		transactions:            []*TransactionCounter{},
		smtLevels:               smtLevels,
		smtLevelsForTransaction: smtLevelsForTransaction,
		blockCount:              0,
		forkId:                  forkId,
		unlimitedCounters:       unlimitedCounters,
		addonCounters:           addonCounters,
	}

	bcc.rlpCombinedCounters = bcc.NewCounters()
	bcc.executionCombinedCounters = bcc.NewCounters()
	bcc.processingCombinedCounters = bcc.NewCounters()

	return &bcc
}

func (bcc *BatchCounterCollector) Clone() *BatchCounterCollector {
	var l2DataCollector *CounterCollector
	txSize := len(bcc.transactions)
	clonedTransactions := make([]*TransactionCounter, txSize)

	for i, tc := range bcc.transactions {
		clonedTransactions[i] = tc.Clone()
	}

	if bcc.l2DataCollector != nil {
		l2DataCollector = bcc.l2DataCollector.Clone()
	}

	return &BatchCounterCollector{
		l2DataCollector:         l2DataCollector,
		transactions:            clonedTransactions,
		smtLevels:               bcc.smtLevels,
		smtLevelsForTransaction: bcc.smtLevelsForTransaction,
		blockCount:              bcc.blockCount,
		forkId:                  bcc.forkId,
		unlimitedCounters:       bcc.unlimitedCounters,

		rlpCombinedCounters:        bcc.rlpCombinedCounters.Clone(),
		executionCombinedCounters:  bcc.executionCombinedCounters.Clone(),
		processingCombinedCounters: bcc.processingCombinedCounters.Clone(),
	}
}

// AddNewTransactionCounters makes the collector aware that a new transaction is attempting to be added to the collector
// here we check the batchL2Data length and ensure that it doesn't cause an overflow.  This will be re-calculated
// every time a new transaction is added as it needs to take into account all the transactions in a batch.
// The function will return false in the case of an error or an overflow of counters
func (bcc *BatchCounterCollector) AddNewTransactionCounters(txCounters *TransactionCounter) (bool, error) {
	err := txCounters.CalculateRlp()
	if err != nil {
		return true, err
	}

	bcc.transactions = append(bcc.transactions, txCounters)
	bcc.UpdateRlpCountersCache(txCounters)

	return bcc.CheckForOverflow(false) //no need to calculate the merkle proof here
}

func (bcc *BatchCounterCollector) RemovePreviousTransactionCounters() {
	lastTx := bcc.transactions[len(bcc.transactions)-1]
	bcc.UndoTransactionCountersCache(lastTx)
	bcc.transactions = bcc.transactions[:len(bcc.transactions)-1]
}

func (bcc *BatchCounterCollector) ClearTransactionCounters() {
	bcc.transactions = bcc.transactions[:0]
}

// StartNewBlock adds in the counters to simulate a changeL2Block transaction.  As these transactions don't really exist
// in a context that isn't the prover we just want to mark down that we have started one.  If adding one causes an overflow we
// return true
func (bcc *BatchCounterCollector) StartNewBlock(verifyMerkleProof bool) (bool, error) {
	bcc.blockCount++
	return bcc.CheckForOverflow(verifyMerkleProof)
}

func (bcc *BatchCounterCollector) processBatchLevelData() error {
	totalEncodedTxLength := 0
	for _, t := range bcc.transactions {
		l2Data, err := t.GetL2DataCache()
		if err != nil {
			return err
		}

		totalEncodedTxLength += len(l2Data)
	}

	// simulate adding in the changeL2Block transactions
	// 1st byte - tx type, always 11 for changeL2Block
	// 2-4 - delta timestamp
	// 5-9 - l1 info tree index
	totalEncodedTxLength += 9 * bcc.blockCount

	// reset the batch processing counters ready to calc the new values
	bcc.l2DataCollector = NewCounterCollector(bcc.smtLevels, bcc.forkId)

	l2Deduction := int(math.Ceil(float64(totalEncodedTxLength+1) / 136))

	bcc.l2DataCollector.Deduct(S, 100)
	bcc.l2DataCollector.Deduct(P, bcc.smtLevels)
	bcc.l2DataCollector.Deduct(B, 2)
	bcc.l2DataCollector.divArith()
	bcc.l2DataCollector.Deduct(K, l2Deduction)
	bcc.l2DataCollector.failAssert()
	bcc.l2DataCollector.consolidateBlock()
	bcc.l2DataCollector.finishBatchProcessing()

	return nil
}

// CheckForOverflow returns true in the case that any counter has less than 0 remaining
func (bcc *BatchCounterCollector) CheckForOverflow(verifyMerkleProof bool) (bool, error) {
	// unlimited counters shouldn't overflow
	if bcc.unlimitedCounters {
		return false, nil
	}
	combined, err := bcc.CombineCollectors(verifyMerkleProof)
	if err != nil {
		return false, err
	}
	overflow := false
	for _, v := range combined {
		if v.remaining < 0 {
			log.Info("[VCOUNTER] Counter overflow detected", "counter", v.name, "remaining", v.remaining, "used", v.used)
			overflow = true
		}
	}

	// if we have an overflow we want to log the counters for debugging purposes
	if overflow {
		logText := "[VCOUNTER] Counters stats"
		for _, v := range combined {
			logText += fmt.Sprintf(" %s: initial: %v used: %v (remaining: %v)", v.name, v.initialAmount, v.used, v.remaining)
		}
		log.Debug(logText)
	}

	return overflow, nil
}

// CounterStats returns a string with combined counter stats.
func (bcc *BatchCounterCollector) CounterStats(verifyMerkleProof bool) (string, error) {
	combined, err := bcc.CombineCollectors(verifyMerkleProof)
	if err != nil {
		return "", err
	}

	// Collect counter stats for logging if overflow is detected
	logText := "[VCOUNTER] Counters stats:"
	for _, v := range combined {
		logText += fmt.Sprintf(" %s: initial: %v, used: %v, remaining: %v;", v.name, v.initialAmount, v.used, v.remaining)
	}

	return logText, nil
}

func (bcc *BatchCounterCollector) NewCounters() Counters {
	var combined Counters
	if bcc.unlimitedCounters {
		combined = *createCountrsByLimits(unlimitedCounters)
	} else {
		combined = *getCounterLimits(bcc.forkId)
	}

	return combined
}

// CombineCollectors takes the batch level data from all transactions and combines these counters with each transactions'
// rlp level counters and execution level counters
func (bcc *BatchCounterCollector) CombineCollectors(verifyMerkleProof bool) (Counters, error) {
	// combine all the counters we have so far

	// if we have external coutners use them, otherwise create new
	// this is used when sequencer starts mid batch and we need the already comulated counters
	combined := bcc.NewCounters()
	if bcc.addonCounters != nil {
		for k, v := range *bcc.addonCounters {
			combined[k].used = v.used
			combined[k].remaining -= v.used
		}
	}

	if err := bcc.processBatchLevelData(); err != nil {
		return nil, err
	}

	// these counter collectors can be re-used for each new block in the batch as they don't rely on inputs
	// from the block or transactions themselves
	changeL2BlockCounter := NewCounterCollector(bcc.smtLevelsForTransaction, bcc.forkId)
	changeL2BlockCounter.processChangeL2Block(verifyMerkleProof)
	changeBlockCounters := NewCounterCollector(bcc.smtLevelsForTransaction, bcc.forkId)
	changeBlockCounters.decodeChangeL2BlockTx()

	// handling changeL2Block counters for each block in the batch - simulating a call to decodeChangeL2BlockTx from the js
	for i := 0; i < bcc.blockCount; i++ {
		for k, v := range changeBlockCounters.counters {
			combined[k].used += v.used
			combined[k].remaining -= v.used
		}

		for k, v := range changeL2BlockCounter.counters {
			combined[k].used += v.used
			combined[k].remaining -= v.used
		}
	}

	if bcc.l2DataCollector != nil {
		for k, v := range bcc.l2DataCollector.Counters() {
			combined[k].used += v.used
			combined[k].remaining -= v.used
		}
	}

	for k := range combined {
		val := bcc.rlpCombinedCounters[k].used + bcc.executionCombinedCounters[k].used + bcc.processingCombinedCounters[k].used
		combined[k].used += val
		combined[k].remaining -= val
	}

	return combined, nil
}

// CombineCollectorsNoChanges takes the batch level data from all transactions and combines these counters with each transactions'
// rlp level counters and execution level counters
// this one returns the counters as they are so far, without adding processBatchLevelData, processChangeL2Block and decodeChangeL2BlockTx
// used to save batch counter progress without adding the said counters twice
func (bcc *BatchCounterCollector) CombineCollectorsNoChanges() Counters {
	// combine all the counters we have so far

	// if we have external coutners use them, otherwise create new
	// this is used when sequencer starts mid batch and we need the already comulated counters
	combined := bcc.NewCounters()
	if bcc.addonCounters != nil {
		for k, v := range *bcc.addonCounters {
			combined[k].used = v.used
			combined[k].remaining -= v.used
		}
	}
	if bcc.l2DataCollector != nil {
		for k, v := range bcc.l2DataCollector.Counters() {
			combined[k].used += v.used
			combined[k].remaining -= v.used
		}
	}

	for _, tx := range bcc.transactions {
		txCounters := tx.CombineCounters()
		for k, v := range txCounters {
			combined[k].used += v.used
			combined[k].remaining -= v.used
		}
	}

	return combined
}

func (bcc *BatchCounterCollector) UpdateRlpCountersCache(txCounters *TransactionCounter) {
	for k, v := range txCounters.rlpCounters.counters {
		bcc.rlpCombinedCounters[k].used += v.used
	}
}

func (bcc *BatchCounterCollector) UpdateExecutionAndProcessingCountersCache(txCounters *TransactionCounter) {
	for k, v := range txCounters.executionCounters.counters {
		bcc.executionCombinedCounters[k].used += v.used
	}

	for k, v := range txCounters.processingCounters.counters {
		bcc.processingCombinedCounters[k].used += v.used
	}
}

func (bcc *BatchCounterCollector) UndoTransactionCountersCache(txCounters *TransactionCounter) {
	for k, v := range txCounters.rlpCounters.counters {
		bcc.rlpCombinedCounters[k].used -= v.used
	}
	for k, v := range txCounters.executionCounters.counters {
		bcc.executionCombinedCounters[k].used -= v.used
	}
	for k, v := range txCounters.processingCounters.counters {
		bcc.processingCombinedCounters[k].used -= v.used
	}
}
