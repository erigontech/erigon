package vm

import (
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/tx"
)

type TransactionCounter struct {
	transaction        types.Transaction
	rlpCounters        *CounterCollector
	executionCounters  *CounterCollector
	processingCounters *CounterCollector
	smtLevels          int
	forkId             uint16
	l2DataCache        []byte
}

func NewTransactionCounter(transaction types.Transaction, smtMaxLevel int, forkId uint16, mcpReduction float64, shouldCountersBeUnlimited bool) *TransactionCounter {
	totalLevel := calculateSmtLevels(smtMaxLevel, 32, mcpReduction)

	var tc *TransactionCounter

	if shouldCountersBeUnlimited {
		tc = &TransactionCounter{
			transaction:        transaction,
			rlpCounters:        NewUnlimitedCounterCollector(),
			executionCounters:  NewUnlimitedCounterCollector(),
			processingCounters: NewUnlimitedCounterCollector(),
			smtLevels:          1, // max depth of the tree anyways
			forkId:             forkId,
		}
	} else {
		tc = &TransactionCounter{
			transaction:        transaction,
			rlpCounters:        NewCounterCollector(totalLevel, forkId),
			executionCounters:  NewCounterCollector(totalLevel, forkId),
			processingCounters: NewCounterCollector(totalLevel, forkId),
			smtLevels:          totalLevel,
			forkId:             forkId,
		}
	}
	tc.executionCounters.SetTransaction(transaction)

	return tc
}

func (tc *TransactionCounter) CombineCounters() Counters {
	combined := NewCounters()
	for k := range tc.rlpCounters.counters {
		val := tc.rlpCounters.counters[k].used + tc.executionCounters.counters[k].used + tc.processingCounters.counters[k].used
		combined[k] = &Counter{
			used: val,
		}
	}

	return combined
}

func (tc *TransactionCounter) Clone() *TransactionCounter {
	var l2DataCacheCopy []byte
	if tc.l2DataCache != nil {
		l2DataCacheCopy = make([]byte, len(tc.l2DataCache))
		copy(l2DataCacheCopy, tc.l2DataCache)
	}

	return &TransactionCounter{
		transaction:        tc.transaction,
		rlpCounters:        tc.rlpCounters.Clone(),
		executionCounters:  tc.executionCounters.Clone(),
		processingCounters: tc.processingCounters.Clone(),
		smtLevels:          tc.smtLevels,
		l2DataCache:        l2DataCacheCopy,
	}
}

func (tc *TransactionCounter) GetL2DataCache() ([]byte, error) {
	if tc.l2DataCache == nil {
		data, err := tx.TransactionToL2Data(tc.transaction, 8, tx.MaxEffectivePercentage)
		if err != nil {
			return data, err
		}

		tc.l2DataCache = data
	}
	return tc.l2DataCache, nil
}

func (tc *TransactionCounter) CalculateRlp() error {
	raw, err := tc.GetL2DataCache()
	if err != nil {
		return err
	}

	gasLimitHex := fmt.Sprintf("%x", tc.transaction.GetGas())
	hexutil.AddLeadingZeroToHexValueForByteCompletion(&gasLimitHex)
	gasPriceHex := tc.transaction.GetPrice().Hex()
	hexutil.Remove0xPrefixIfExists(&gasPriceHex)
	hexutil.AddLeadingZeroToHexValueForByteCompletion(&gasPriceHex)
	valueHex := tc.transaction.GetValue().Hex()
	hexutil.Remove0xPrefixIfExists(&valueHex)
	hexutil.AddLeadingZeroToHexValueForByteCompletion(&valueHex)
	chainIdHex := tc.transaction.GetChainID().Hex()
	hexutil.Remove0xPrefixIfExists(&chainIdHex)
	hexutil.AddLeadingZeroToHexValueForByteCompletion(&chainIdHex)
	nonceHex := fmt.Sprintf("%x", tc.transaction.GetNonce())
	hexutil.AddLeadingZeroToHexValueForByteCompletion(&nonceHex)

	txRlpLength := len(raw)
	txDataLen := len(tc.transaction.GetData())
	gasLimitLength := len(gasLimitHex) / 2
	gasPriceLength := len(gasPriceHex) / 2
	valueLength := len(valueHex) / 2
	chainIdLength := len(chainIdHex) / 2
	nonceLength := len(nonceHex) / 2

	collector := NewCounterCollector(tc.smtLevels, tc.forkId)
	collector.Deduct(S, 250)
	collector.Deduct(B, 1+1)
	collector.Deduct(K, int(math.Ceil(float64(txRlpLength+1)/136)))
	collector.Deduct(P, int(math.Ceil(float64(txRlpLength+1)/56)+3))
	collector.Deduct(D, int(math.Ceil(float64(txRlpLength+1)/56)+3))
	collector.multiCall(collector.addBatchHashData, 21)
	/**
	from the original JS implementation:

	 * We need to calculate the counters consumption of `_checkNonLeadingZeros`, which calls `_getLenBytes`
	 * _checkNonLeadingZeros is called 7 times
	 * The worst case scenario each time `_checkNonLeadingZeros`+ `_getLenBytes` is called is the following:
	 * readList -> approx 300000 bytes -> the size can be expressed with 3 bytes -> len(hex(300000)) = 3 bytes
	 * gasPrice -> 256 bits -> 32 bytes
	 * gasLimit -> 64 bits -> 8 bytes
	 * value -> 256 bits -> 32 bytes
	 * dataLen -> 300000 bytes -> xxxx bytes
	 * chainId -> 64 bits -> 8 bytes
	 * nonce -> 64 bits -> 8 bytes
	*/
	collector.Deduct(S, 6*7) // Steps to call _checkNonLeadingZeros 7 times

	// inside a little forEach in the JS implementation
	collector.getLenBytes(3)
	collector.getLenBytes(gasPriceLength)
	collector.getLenBytes(gasLimitLength)
	collector.getLenBytes(valueLength)
	if txDataLen >= 56 {
		collector.getLenBytes(txDataLen)
	}
	collector.getLenBytes(chainIdLength)
	collector.getLenBytes(nonceLength)

	collector.divArith()
	collector.multiCall(collector.addHashTx, 9+(txDataLen>>5)) //txDataLen>>5 equals to int(math.Floor(float64(txDataLen)/32))
	collector.multiCall(collector.addL2HashTx, 8+(txDataLen>>5))
	collector.multiCall(collector.addBatchHashByteByByte, txDataLen)
	collector.SHLarith()

	v, r, s := tc.transaction.RawSignatureValues()
	v = tx.GetDecodedV(tc.transaction, v)
	if err := collector.ecRecover(v, r, s, false); err != nil {
		return err
	}

	tc.rlpCounters = collector

	return nil
}

func (tc *TransactionCounter) ProcessTx(ibs *state.IntraBlockState, returnData []byte) error {
	byteCodeLength := 0
	isDeploy := false
	toAddress := tc.transaction.GetTo()
	if toAddress == nil {
		byteCodeLength = len(returnData)
		isDeploy = true
	} else {
		byteCodeLength = ibs.GetCodeSize(*toAddress)
	}

	cc := NewCounterCollector(tc.smtLevels, tc.forkId)
	cc.Deduct(S, 300)
	if tc.forkId >= uint16(chain.ForkId13Durian) {
		cc.Deduct(B, 12+7)
	} else {
		cc.Deduct(B, 11+7)
	}
	cc.Deduct(P, 14*tc.smtLevels)
	cc.Deduct(D, 5)
	cc.Deduct(A, 2)
	cc.Deduct(K, 1)
	cc.multiCall(cc.isColdAddress, 2)
	cc.multiCall(cc.addArith, 3)
	cc.subArith()
	cc.divArith()
	cc.multiCall(cc.mulArith, 4)
	cc.fillBlockInfoTreeWithTxReceipt()

	// we always send false for isCreate and isCreate2 here as the original JS does the same
	cc.processContractCall(tc.smtLevels, byteCodeLength, isDeploy, false, false)

	tc.processingCounters = cc

	return nil
}

func (tc *TransactionCounter) ExecutionCounters() *CounterCollector {
	return tc.executionCounters
}

func (tc *TransactionCounter) ProcessingCounters() *CounterCollector {
	return tc.processingCounters
}
