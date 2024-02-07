package vm

import (
	"bytes"
	"math"
	"strconv"
)

type BatchCounterCollector struct {
	// every new transaction added to the batch affects these counters, so they need their own entry as this will
	// be overwritten time and again as transactions are added
	l2DataCollector *CounterCollector
	transactions    []*TransactionCounter
	smtLevels       int
	blockCount      int
}

func NewBatchCounterCollector(smtMaxLevel uint32) *BatchCounterCollector {
	totalLevel := len(strconv.FormatInt(int64(math.Pow(2, float64(smtMaxLevel))+250000), 2))
	return &BatchCounterCollector{
		transactions: []*TransactionCounter{},
		smtLevels:    totalLevel,
		blockCount:   0,
	}
}

// AddNewTransactionCounters makes the collector aware that a new transaction is attempting to be added to the collector
// here we check the batchL2Data length and ensure that it doesn't cause an overflow.  This will be re-calculated
// every time a new transaction is added as it needs to take into account all the transactions in a batch.
// The function will return false in the case of an error or an overflow of counters
func (bcc *BatchCounterCollector) AddNewTransactionCounters(txCounters *TransactionCounter) (bool, error) {
	bcc.transactions = append(bcc.transactions, txCounters)

	if err := bcc.processBatchLevelData(); err != nil {
		return true, err
	}

	err := txCounters.CalculateRlp()
	if err != nil {
		return true, err
	}

	return bcc.CheckForOverflow(), nil
}

// StartNewBlock adds in the counters to simulate a changeL2Block transaction.  As these transactions don't really exist
// in a context that isn't the prover we just want to mark down that we have started one.  If adding one causes an overflow we
// return true
func (bcc *BatchCounterCollector) StartNewBlock() bool {
	bcc.blockCount++
	return bcc.CheckForOverflow()
}

func (bcc *BatchCounterCollector) processBatchLevelData() error {
	var rlpBytes []byte
	buffer := bytes.NewBuffer(rlpBytes)
	totalRlpLength := 0
	for _, t := range bcc.transactions {
		buffer.Reset()
		err := t.transaction.EncodeRLP(buffer)
		if err != nil {
			return err
		}
		totalRlpLength += len(rlpBytes)
	}

	// reset the batch processing counters ready to calc the new values
	bcc.l2DataCollector = NewCounterCollector(bcc.smtLevels)

	batchL2DataSize := (totalRlpLength - len(bcc.transactions)*2) / 2
	l2Deduction := int(math.Ceil(float64(batchL2DataSize+1) / 136))

	bcc.l2DataCollector.Deduct(S, 100)
	bcc.l2DataCollector.Deduct(P, bcc.smtLevels)
	bcc.l2DataCollector.divArith()
	bcc.l2DataCollector.Deduct(K, l2Deduction)
	bcc.l2DataCollector.failAssert()
	bcc.l2DataCollector.consolidateBlock()
	bcc.l2DataCollector.finishBatchProcessing(bcc.smtLevels)

	return nil
}

// CheckForOverflow returns true in the case that any counter has less than 0 remaining
func (bcc *BatchCounterCollector) CheckForOverflow() bool {
	combined := bcc.CombineCollectors()
	for _, v := range combined {
		if v.remaining < 0 {
			return true
		}
	}
	return false
}

// CombineCollectors takes the batch level data from all transactions and combines these counters with each transactions'
// rlp level counters and execution level counters
func (bcc *BatchCounterCollector) CombineCollectors() Counters {
	// combine all the counters we have so far
	combined := defaultCounters()

	// these counter collectors can be re-used for each new block in the batch as they don't rely on inputs
	// from the block or transactions themselves
	changeL2BlockCounter := NewCounterCollector(bcc.smtLevels)
	changeL2BlockCounter.processChangeL2Block()
	changeBlockCounters := NewCounterCollector(bcc.smtLevels)
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

	for _, tx := range bcc.transactions {
		for k, v := range tx.rlpCounters.counters {
			combined[k].used += v.used
			combined[k].remaining -= v.used
		}
		for k, v := range tx.executionCounters.counters {
			combined[k].used += v.used
			combined[k].remaining -= v.used
		}
		for k, v := range tx.processingCounters.counters {
			combined[k].used += v.used
			combined[k].remaining -= v.used
		}
	}

	return combined
}
