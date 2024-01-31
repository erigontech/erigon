package vm

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types"
	"math"
)

var ErrZkCounterOverspend = errors.New("virtual zk counters overspend")

var totalSteps = math.Pow(2, 23)

const (
	MCPL    = 23
	fnecHex = "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141"
	fpecHex = "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F"
)

type Counter struct {
	remaining     int
	used          int
	name          string
	initialAmount int
}

type Counters map[CounterKey]*Counter

type CounterKey string

var (
	S CounterKey = "S"
	A CounterKey = "A"
	B CounterKey = "B"
	M CounterKey = "M"
	K CounterKey = "K"
	D CounterKey = "D"
	P CounterKey = "P"
)

type TransactionCounter struct {
	transaction       types.Transaction
	rlpCounters       *CounterCollector
	executionCounters *CounterCollector
}

func NewTransactionCounter(transaction types.Transaction) *TransactionCounter {
	tc := &TransactionCounter{
		transaction:       transaction,
		rlpCounters:       NewCounterCollector(),
		executionCounters: NewCounterCollector(),
	}

	return tc
}

func (tc *TransactionCounter) CalculateRlp() error {
	var rlpBytes []byte
	buffer := bytes.NewBuffer(rlpBytes)
	err := tc.transaction.EncodeRLP(buffer)
	if err != nil {
		return err
	}

	gasLimitHex := fmt.Sprintf("%x", tc.transaction.GetGas())
	gasPriceHex := fmt.Sprintf("%x", tc.transaction.GetPrice().Uint64())
	valueHex := fmt.Sprintf("%x", tc.transaction.GetValue().Uint64())
	chainIdHex := fmt.Sprintf("%x", tc.transaction.GetChainID().Uint64())
	nonceHex := fmt.Sprintf("%x", tc.transaction.GetNonce())

	txRlpLength := len(rlpBytes)
	txDataLen := len(rlpBytes)
	gasLimitLength := len(gasLimitHex) / 2
	gasPriceLength := len(gasPriceHex) / 2
	valueLength := len(valueHex) / 2
	chainIdLength := len(chainIdHex) / 2
	nonceLength := len(nonceHex) / 2

	collector := NewCounterCollector()
	collector.Deduct(S, 250)
	collector.Deduct(B, 1+1)
	collector.Deduct(K, int(math.Ceil(float64(txRlpLength+1)/136)))
	collector.Deduct(P, int(math.Ceil(float64(txRlpLength+1)/56)))
	collector.Deduct(D, int(math.Ceil(float64(txRlpLength+1)/56)))
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
	collector.getLenBytes(txDataLen)
	collector.getLenBytes(chainIdLength)
	collector.getLenBytes(nonceLength)

	collector.divArith()
	collector.multiCall(collector.addHashTx, 9+int(math.Floor(float64(txDataLen)/32)))
	collector.multiCall(collector.addL2HashTx, 8+int(math.Floor(float64(txDataLen)/32)))
	collector.multiCall(collector.addBatchHashByteByByte, txDataLen)
	collector.SHLarith()

	v, r, s := tc.transaction.RawSignatureValues()
	err = collector.ecRecover(v, r, s, false)
	if err != nil {
		return err
	}

	tc.rlpCounters = collector

	return nil
}

func (tc *TransactionCounter) DecodeChangeL2Block() {
	collector := NewCounterCollector()
	collector.Deduct(S, 20)
	collector.multiCall(collector.addBatchHashData, 3)
	tc.rlpCounters = collector
}

func (tc *TransactionCounter) ExecutionCounters() *CounterCollector {
	return tc.executionCounters
}

type CounterManager struct {
	currentCounters    Counters
	currentTransaction types.Transaction
	historicalCounters []Counters
	calls              [256]executionFunc
	smtMaxLevel        int64
	smtLevels          int
	transactionStore   []types.Transaction
}

type CounterCollector struct {
	counters Counters
}

func NewCounterCollector() *CounterCollector {
	return &CounterCollector{
		counters: defaultCounters(),
	}
}

func (cc *CounterCollector) Deduct(key CounterKey, amount int) {
	cc.counters[key].used += amount
	cc.counters[key].remaining -= amount
}

func defaultCounters() Counters {
	return Counters{
		S: {
			remaining:     int(totalSteps),
			name:          "totalSteps",
			initialAmount: int(totalSteps),
		},
		A: {
			remaining:     int(math.Floor(totalSteps / 32)),
			name:          "arith",
			initialAmount: int(math.Floor(totalSteps / 32)),
		},
		B: {
			remaining:     int(math.Floor(totalSteps / 16)),
			name:          "binary",
			initialAmount: int(math.Floor(totalSteps / 16)),
		},
		M: {
			remaining:     int(math.Floor(totalSteps / 32)),
			name:          "memAlign",
			initialAmount: int(math.Floor(totalSteps / 32)),
		},
		K: {
			remaining:     int(math.Floor(totalSteps/155286) * 44),
			name:          "keccaks",
			initialAmount: int(math.Floor(totalSteps/155286) * 44),
		},
		D: {
			remaining:     int(math.Floor(totalSteps / 56)),
			name:          "padding",
			initialAmount: int(math.Floor(totalSteps / 56)),
		},
		P: {
			remaining:     int(math.Floor(totalSteps / 30)),
			name:          "poseidon",
			initialAmount: int(math.Floor(totalSteps / 30)),
		},
	}
}

func (cc *CounterCollector) Counters() Counters {
	return cc.counters
}

func WrapJumpTableWithZkCounters(originalTable *JumpTable, counterCalls [256]executionFunc) *JumpTable {
	wrapper := func(original, counter executionFunc) executionFunc {
		return func(p *uint64, i *EVMInterpreter, s *ScopeContext) ([]byte, error) {
			b, err := counter(p, i, s)
			if err != nil {
				return b, err
			}
			return original(p, i, s)
		}
	}

	result := &JumpTable{}

	for idx := range originalTable {
		original := originalTable[idx]
		// if we have something in the Counter table to process wrap the function call
		if counterCalls[idx] != nil {
			originalExec := originalTable[idx].execute
			counterExec := counterCalls[idx]
			wrappedExec := wrapper(originalExec, counterExec)
			original.execute = wrappedExec
		}
		result[idx] = original
	}

	return result
}

func SimpleCounterOperations(cc *CounterCollector) [256]executionFunc {
	calls := [256]executionFunc{
		ADD: cc.counterOpAdd,
	}
	return calls
}

func (cc *CounterCollector) counterOpAdd(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) mLoadX() {
	cc.Deduct(S, 40)
	cc.Deduct(B, 2)
	cc.Deduct(M, 1)
	cc.offsetUtil()
	cc.SHRarith()
	cc.SHLarith()
}

func (cc *CounterCollector) offsetUtil() {
	cc.Deduct(S, 12)
	cc.Deduct(B, 1)
}

func (cc *CounterCollector) SHRarith() {
	cc.Deduct(S, 50)
	cc.Deduct(B, 2)
	cc.Deduct(A, 1)
	cc.divArith()
}

func (cc *CounterCollector) SHLarith() {
	cc.Deduct(S, 100)
	cc.Deduct(B, 4)
	cc.Deduct(A, 2)
}

func (cc *CounterCollector) divArith() {
	cc.Deduct(S, 50)
	cc.Deduct(B, 3)
	cc.Deduct(A, 1)
}

func (cc *CounterCollector) opCode(isCreate bool) {
	cc.Deduct(S, 12)
	if isCreate {
		cc.mLoadX()
		cc.SHRarith()
	}
}

func (cc *CounterCollector) addBatchHashData() {
	cc.Deduct(S, 10)
}

func (cc *CounterCollector) getLenBytes(l int) {
	cc.Deduct(S, l*7+12)
	cc.Deduct(B, l*1)
	cc.multiCall(cc.SHRarith, l)
}

func (cc *CounterCollector) addHashTx() {
	cc.Deduct(S, 10)
}

func (cc *CounterCollector) addL2HashTx() {
	cc.Deduct(S, 10)
}

func (cc *CounterCollector) addBatchHashByteByByte() {
	cc.Deduct(S, 25)
	cc.Deduct(B, 1)
	cc.SHRarith()
	cc.addBatchHashData()
}

func (cc *CounterCollector) ecRecover(v, r, s *uint256.Int, isPrecompiled bool) error {
	var upperLimit *uint256.Int
	fnec, err := uint256.FromHex(fnecHex)
	if err != nil {
		return err
	}
	fnecMinusOne := fnec.Sub(fnec, uint256.NewInt(1))
	if isPrecompiled {
		upperLimit = fnecMinusOne
	} else {
		upperLimit = fnec.Div(fnec, uint256.NewInt(2))
	}

	// handle a dodgy signature
	if r.Uint64() == 0 || fnecMinusOne.Lt(r) || s.Uint64() == 0 || upperLimit.Lt(s) || v.Uint64() != 27 && v.Uint64() != 28 {
		cc.Deduct(S, 45)
		cc.Deduct(B, 8)
		cc.Deduct(A, 2)
		return nil
	}

	fpec, err := uint256.FromHex(fpecHex)
	if err != nil {
		return err
	}

	// check if we have a sqrt to avoid counters at checkSqrtFpEc (from js)
	c := uint256.NewInt(0)
	rExp := r.Clone().Exp(r, uint256.NewInt(3))
	c.Mod(
		uint256.NewInt(0).Add(rExp, uint256.NewInt(7)),
		fpec,
	)

	r2 := fpec.Clone().Sqrt(c)
	var parity uint64 = 1
	if v.Uint64() == 27 {
		parity = 0
	}
	if r2 != nil {
		r2, err = uint256.FromHex("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
		if err != nil {
			return err
		}
	} else if r2.Uint64()&1 != parity {
		r2 = fpec.Clone().Neg(r)
	}

	if r2.Lt(fpec) {
		// do not have a root
		cc.Deduct(S, 4527)
		cc.Deduct(A, 1014)
		cc.Deduct(B, 10)
	} else {
		// has a root
		cc.Deduct(S, 6294)
		cc.Deduct(A, 528)
		cc.Deduct(B, 523)
		cc.Deduct(K, 1)
	}

	return nil
}

func (cc *CounterCollector) failAssert() {
	cc.Deduct(S, 2)
}

func (cc *CounterCollector) consolidateBlock() {
	cc.Deduct(S, 20)
	cc.Deduct(B, 2)
	cc.Deduct(P, 2*MCPL)
}

func (cc *CounterCollector) finishBatchProcessing(smtLevels int) {
	cc.Deduct(S, 200)
	cc.Deduct(K, 2)
	cc.Deduct(P, smtLevels)
	cc.Deduct(B, 1)
}

func (cc *CounterCollector) decodeChangeL2Block() {
	cc.Deduct(S, 20)
	cc.multiCall(cc.addBatchHashData, 3)
}

func (cc *CounterCollector) invFnEc() {
	cc.Deduct(S, 12)
	cc.Deduct(B, 2)
	cc.Deduct(A, 2)
}

func (cc *CounterCollector) multiCall(call func(), times int) {
	for i := 0; i < times; i++ {
		call()
	}
}
