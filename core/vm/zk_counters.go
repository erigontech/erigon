package vm

import (
	"fmt"
	"math"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

const (
	MCPL    = 23
	fnecHex = "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141"
	fpecHex = "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F"
)

var (
	fnec         *uint256.Int
	fnecMinusOne *uint256.Int
	fnecHalf     *uint256.Int
	fpec         *uint256.Int
)

func init() {
	var err error
	fnec, err = uint256.FromHex(fnecHex)
	if err != nil {
		panic("could not parse fnec hex as uint256")
	}

	fnecMinusOne = fnec.Clone().Sub(fnec, uint256.NewInt(1))
	fnecHalf = fnec.Clone().Div(fnec, uint256.NewInt(2))

	fpec, err = uint256.FromHex(fpecHex)
	if err != nil {
		panic("could not parse fpec as uint256")
	}
}

type Counter struct {
	remaining     int
	used          int
	name          string
	initialAmount int
}

func (c *Counter) Clone() *Counter {
	return &Counter{
		remaining:     c.remaining,
		used:          c.used,
		name:          c.name,
		initialAmount: c.initialAmount,
	}
}

func (c *Counter) Used() int { return c.used }

func (c *Counter) Limit() int { return c.initialAmount }

func (c *Counter) AsMap() map[string]int {
	return map[string]int{
		"remaining":     c.remaining,
		"used":          c.used,
		"initialAmount": c.initialAmount,
	}
}

type Counters []*Counter

func NewCounters() Counters {
	array := make(Counters, CounterTypesCount)
	return array
}

func NewCountersFromUsedArray(used []int) *Counters {
	res := Counters{}
	for k, v := range used {
		res[k] = &Counter{used: v}
	}
	return &res
}

func (c Counters) UsedAsString() string {
	res := fmt.Sprintf("[%s: %v]", CounterKeyNames[SHA], c[SHA].used)
	res += fmt.Sprintf("[%s: %v]", CounterKeyNames[A], c[A].used)
	res += fmt.Sprintf("[%s: %v]", CounterKeyNames[B], c[B].used)
	res += fmt.Sprintf("[%s: %v]", CounterKeyNames[K], c[K].used)
	res += fmt.Sprintf("[%s: %v]", CounterKeyNames[M], c[M].used)
	res += fmt.Sprintf("[%s: %v]", CounterKeyNames[P], c[P].used)
	res += fmt.Sprintf("[%s: %v]", CounterKeyNames[S], c[S].used)
	res += fmt.Sprintf("[%s: %v]", CounterKeyNames[D], c[D].used)
	return res
}

func (c Counters) UsedAsArray() []int {
	array := make([]int, len(c))

	for i, v := range c {
		array[i] = v.used
	}

	return array
}

func (c Counters) UsedAsMap() map[string]int {
	return map[string]int{
		string(CounterKeyNames[S]):   c[S].used,
		string(CounterKeyNames[A]):   c[A].used,
		string(CounterKeyNames[B]):   c[B].used,
		string(CounterKeyNames[M]):   c[M].used,
		string(CounterKeyNames[K]):   c[K].used,
		string(CounterKeyNames[D]):   c[D].used,
		string(CounterKeyNames[P]):   c[P].used,
		string(CounterKeyNames[SHA]): c[SHA].used,
	}
}

func (c *Counters) GetArithmetics() *Counter {
	return (*c)[A]
}

func (c *Counters) GetBinaries() *Counter {
	return (*c)[B]
}

func (c *Counters) GetSHA256Hashes() *Counter {
	return (*c)[SHA]
}

func (c *Counters) GetKeccakHashes() *Counter {
	return (*c)[K]
}

func (c *Counters) GetMemAligns() *Counter {
	return (*c)[M]
}

func (c *Counters) GetPoseidonHashes() *Counter {
	return (*c)[P]
}

func (c *Counters) GetSteps() *Counter {
	return (*c)[S]
}

func (c *Counters) GetPoseidonPaddings() *Counter {
	return (*c)[D]
}

func (cc Counters) Clone() Counters {
	var clonedCounters Counters = Counters{}

	for k, v := range cc {
		clonedCounters[k] = v.Clone()
	}

	return clonedCounters
}

type CounterKey int
type CounterName string

const (
	S   CounterKey = 0
	A   CounterKey = 1
	B   CounterKey = 2
	M   CounterKey = 3
	K   CounterKey = 4
	D   CounterKey = 5
	P   CounterKey = 6
	SHA CounterKey = 7

	CounterTypesCount = 8
)

var (
	// important!!! must match the indexes of the keys
	CounterKeyNames = []CounterName{"S", "A", "B", "M", "K", "D", "P", "SHA"}
)

type CounterCollector struct {
	counters    Counters
	smtLevels   int
	isDeploy    bool
	transaction types.Transaction
}

func (cc *CounterCollector) isOverflown() bool {
	for _, v := range cc.counters {
		if v.remaining < 0 {
			return true
		}
	}
	return false
}

func calculateSmtLevels(smtMaxLevel int, minValue int, mcpReduction float64) int {
	binary := big.NewInt(0)
	base := big.NewInt(2)
	power := big.NewInt(int64(smtMaxLevel))
	offset := big.NewInt(50000)

	binary.Exp(base, power, nil)
	binary.Add(binary, offset)
	binaryLength := len(fmt.Sprintf("%b", binary))

	if binaryLength < minValue {
		binaryLength = minValue
	}

	binaryLength = int(math.Floor(float64(binaryLength) * mcpReduction))

	return binaryLength
}

func NewUnlimitedCounterCollector() *CounterCollector {
	return &CounterCollector{
		counters:  *createCountrsByLimits(unlimitedCounters),
		smtLevels: 256,
	}
}

func NewCounterCollector(smtLevels int, forkId uint16) *CounterCollector {
	return &CounterCollector{
		counters:  *getCounterLimits(forkId),
		smtLevels: smtLevels,
	}
}

func (cc *CounterCollector) Clone() *CounterCollector {
	var clonedCounters Counters = Counters{}

	for k, v := range cc.counters {
		clonedCounters[k] = v.Clone()
	}

	return &CounterCollector{
		counters:    clonedCounters,
		smtLevels:   cc.smtLevels,
		isDeploy:    cc.isDeploy,
		transaction: cc.transaction, // no need to make deep clone of a transaction
	}
}

func (cc *CounterCollector) GetSmtLevels() int {
	return cc.smtLevels
}

func (cc *CounterCollector) Deduct(key CounterKey, amount int) {
	cc.counters[key].used += amount
	cc.counters[key].remaining -= amount
}

func (cc *CounterCollector) Counters() Counters {
	return cc.counters
}

func (cc *CounterCollector) SetTransaction(transaction types.Transaction) {
	cc.transaction = transaction
	cc.isDeploy = transaction.IsContractDeploy()
}

func WrapJumpTableWithZkCounters(originalTable *JumpTable, counterCalls *[256]executionFunc) *JumpTable {
	wrapper := func(original, counter executionFunc) executionFunc {
		return func(p *uint64, i *EVMInterpreter, s *ScopeContext) ([]byte, error) {
			if _, err := counter(p, i, s); err != nil {
				return nil, err
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

// func WrapJumpTableWithTracerCounters(originalTable *JumpTable) *JumpTable {
// 	wrapper := func(original, counter executionFunc) executionFunc {
// 		return func(p *uint64, i *EVMInterpreter, s *ScopeContext) ([]byte, error) {
// 			b, err := counter(p, i, s)
// 			if err != nil {
// 				return b, err
// 			}
// 			return original(p, i, s)
// 		}
// 	}

// 	result := &JumpTable{}

// 	for idx := range originalTable {
// 		original := originalTable[idx]
// 		// if we have something in the Counter table to process wrap the function call
// 		if counterCalls[idx] != nil {
// 			originalExec := originalTable[idx].execute
// 			counterExec := counterCalls[idx]
// 			wrappedExec := wrapper(originalExec, counterExec)
// 			original.execute = wrappedExec
// 		}
// 		result[idx] = original
// 	}

// 	return result
// }

func SimpleCounterOperations(cc *CounterCollector) *[256]executionFunc {
	calls := &[256]executionFunc{
		ADD:            cc.opAdd,
		MUL:            cc.opMul,
		SUB:            cc.opSub,
		DIV:            cc.opDiv,
		SDIV:           cc.opSDiv,
		MOD:            cc.opMod,
		SMOD:           cc.opSMod,
		ADDMOD:         cc.opAddMod,
		MULMOD:         cc.opMulMod,
		EXP:            cc.opExp,
		SIGNEXTEND:     cc.opSignExtend,
		BLOCKHASH:      cc.opBlockHash,
		COINBASE:       cc.opCoinbase,
		TIMESTAMP:      cc.opTimestamp,
		NUMBER:         cc.opNumber,
		DIFFICULTY:     cc.opDifficulty,
		GASLIMIT:       cc.opGasLimit,
		CHAINID:        cc.opChainId,
		CALLDATALOAD:   cc.opCalldataLoad,
		CALLDATASIZE:   cc.opCalldataSize,
		CALLDATACOPY:   cc.opCalldataCopy,
		CODESIZE:       cc.opCodeSize,
		EXTCODESIZE:    cc.opExtCodeSize,
		EXTCODECOPY:    cc.opExtCodeCopy,
		CODECOPY:       cc.opCodeCopy,
		RETURNDATASIZE: cc.opReturnDataSize,
		RETURNDATACOPY: cc.opReturnDataCopy,
		EXTCODEHASH:    cc.opExtCodeHash,
		LT:             cc.opLT,
		GT:             cc.opGT,
		SLT:            cc.opSLT,
		SGT:            cc.opSGT,
		EQ:             cc.opEQ,
		ISZERO:         cc.opIsZero,
		AND:            cc.opAnd,
		OR:             cc.opOr,
		XOR:            cc.opXor,
		NOT:            cc.opNot,
		BYTE:           cc.opByte,
		SHR:            cc.opSHR,
		SHL:            cc.opSHL,
		SAR:            cc.opSAR,
		STOP:           cc.opStop,
		CREATE:         cc.opCreate,
		CALL:           cc.opCall,
		CALLCODE:       cc.opCallCode,
		DELEGATECALL:   cc.opDelegateCall,
		STATICCALL:     cc.opStaticCall,
		CREATE2:        cc.opCreate2,
		RETURN:         cc.opReturn,
		REVERT:         cc.opRevert,
		SELFDESTRUCT:   cc.opSendAll,
		INVALID:        cc.opInvalid,
		ADDRESS:        cc.opAddress,
		SELFBALANCE:    cc.opSelfBalance,
		ORIGIN:         cc.opOrigin,
		CALLER:         cc.opCaller,
		CALLVALUE:      cc.opCallValue,
		GASPRICE:       cc.opGasPrice,
		GAS:            cc.opGas,
		KECCAK256:      cc.opSha3,
		JUMP:           cc.opJump,
		JUMPI:          cc.opJumpI,
		PC:             cc.opPC,
		JUMPDEST:       cc.opJumpDest,
		LOG0:           cc.opLog0,
		LOG1:           cc.opLog1,
		LOG2:           cc.opLog2,
		LOG3:           cc.opLog3,
		LOG4:           cc.opLog4,
		PUSH0:          cc.opPush0,
		PUSH1:          cc.opPushGenerator(1),
		PUSH2:          cc.opPushGenerator(2),
		PUSH3:          cc.opPushGenerator(3),
		PUSH4:          cc.opPushGenerator(4),
		PUSH5:          cc.opPushGenerator(5),
		PUSH6:          cc.opPushGenerator(6),
		PUSH7:          cc.opPushGenerator(7),
		PUSH8:          cc.opPushGenerator(8),
		PUSH9:          cc.opPushGenerator(9),
		PUSH10:         cc.opPushGenerator(10),
		PUSH11:         cc.opPushGenerator(11),
		PUSH12:         cc.opPushGenerator(12),
		PUSH13:         cc.opPushGenerator(13),
		PUSH14:         cc.opPushGenerator(14),
		PUSH15:         cc.opPushGenerator(15),
		PUSH16:         cc.opPushGenerator(16),
		PUSH17:         cc.opPushGenerator(17),
		PUSH18:         cc.opPushGenerator(18),
		PUSH19:         cc.opPushGenerator(19),
		PUSH20:         cc.opPushGenerator(20),
		PUSH21:         cc.opPushGenerator(21),
		PUSH22:         cc.opPushGenerator(22),
		PUSH23:         cc.opPushGenerator(23),
		PUSH24:         cc.opPushGenerator(24),
		PUSH25:         cc.opPushGenerator(25),
		PUSH26:         cc.opPushGenerator(26),
		PUSH27:         cc.opPushGenerator(27),
		PUSH28:         cc.opPushGenerator(28),
		PUSH29:         cc.opPushGenerator(29),
		PUSH30:         cc.opPushGenerator(30),
		PUSH31:         cc.opPushGenerator(31),
		PUSH32:         cc.opPushGenerator(32),
		DUP1:           cc.opDup,
		DUP2:           cc.opDup,
		DUP3:           cc.opDup,
		DUP4:           cc.opDup,
		DUP5:           cc.opDup,
		DUP6:           cc.opDup,
		DUP7:           cc.opDup,
		DUP8:           cc.opDup,
		DUP9:           cc.opDup,
		DUP10:          cc.opDup,
		DUP11:          cc.opDup,
		DUP12:          cc.opDup,
		DUP13:          cc.opDup,
		DUP14:          cc.opDup,
		DUP15:          cc.opDup,
		DUP16:          cc.opDup,
		SWAP1:          cc.opSwap,
		SWAP2:          cc.opSwap,
		SWAP3:          cc.opSwap,
		SWAP4:          cc.opSwap,
		SWAP5:          cc.opSwap,
		SWAP6:          cc.opSwap,
		SWAP7:          cc.opSwap,
		SWAP8:          cc.opSwap,
		SWAP9:          cc.opSwap,
		SWAP10:         cc.opSwap,
		SWAP11:         cc.opSwap,
		SWAP12:         cc.opSwap,
		SWAP13:         cc.opSwap,
		SWAP14:         cc.opSwap,
		SWAP15:         cc.opSwap,
		SWAP16:         cc.opSwap,
		POP:            cc.opPop,
		MLOAD:          cc.opMLoad,
		MSTORE:         cc.opMStore,
		MSTORE8:        cc.opMStore8,
		MSIZE:          cc.opMSize,
		SLOAD:          cc.opSLoad,
		SSTORE:         cc.opSSTore,
		BALANCE:        cc.opBalance,
	}
	return calls
}

func (cc *CounterCollector) mLoadX() {
	cc.Deduct(S, 30)
	cc.Deduct(B, 2)
	cc.Deduct(M, 1)
	cc.offsetUtil()
	cc.SHRarith()
	cc.SHLarith()
}

func (cc *CounterCollector) offsetUtil() {
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
}

func (cc *CounterCollector) SHRarith() {
	cc.Deduct(S, 40)
	cc.Deduct(B, 2)
	cc.Deduct(A, 1)
	cc.divArith()
}

func (cc *CounterCollector) SHLarith() {
	cc.Deduct(S, 40)
	cc.Deduct(B, 2)
	cc.Deduct(A, 2)
}

func (cc *CounterCollector) divArith() {
	cc.Deduct(S, 40)
	cc.Deduct(B, 3)
	cc.Deduct(A, 1)
}

func (cc *CounterCollector) opCode(scope *ScopeContext) {
	cc.Deduct(S, 12)
	if scope.Contract.IsCreate || scope.Contract.IsCreate2 || cc.isDeploy {
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
	upperLimit := fnecMinusOne
	if !isPrecompiled {
		upperLimit = fnecHalf
	}

	// handle a dodgy signature
	fnecCheck := fnecMinusOne.Lt(r)
	upperLimitCheck := upperLimit.Lt(s)
	if r.Uint64() == 0 || s.Uint64() == 0 || fnecCheck || upperLimitCheck || (v.Uint64() != 27 && v.Uint64() != 28) {
		cc.Deduct(S, 45)
		cc.Deduct(A, 2)
		cc.Deduct(B, 8)
		return nil
	}

	// check if we have a sqrt to avoid counters at checkSqrtFpEc (from js)
	c := uint256.NewInt(0)
	rExp := r.Clone().Exp(r, uint256.NewInt(3))
	c.Mod(
		uint256.NewInt(0).Add(rExp, uint256.NewInt(7)),
		fpec,
	)

	var err error
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

	// in js this is converting a boolean to a number and checking for 0 on the less-than check
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

func (cc *CounterCollector) finishBatchProcessing() {
	cc.Deduct(S, 200)
	cc.Deduct(K, 2)
	cc.Deduct(P, cc.smtLevels)
	cc.Deduct(B, 2)
}

func (cc *CounterCollector) isColdAddress() {
	cc.Deduct(S, 100)
	cc.Deduct(B, 2+1)
	cc.Deduct(P, 2*MCPL)
}

func (cc *CounterCollector) addArith() {
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
}

func (cc *CounterCollector) subArith() {
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
}

func (cc *CounterCollector) mulArith() {
	cc.Deduct(S, 40)
	cc.Deduct(B, 1)
	cc.Deduct(A, 1)
}

func (cc *CounterCollector) fillBlockInfoTreeWithTxReceipt() {
	cc.Deduct(S, 20)
	cc.Deduct(P, 3*MCPL)
}

func (cc *CounterCollector) processContractCall(smtLevels int, bytecodeLength int, isDeploy bool, isCreate bool, isCreate2 bool) {
	cc.Deduct(S, 40)
	cc.Deduct(B, 4+1)
	cc.Deduct(P, 1)
	cc.Deduct(D, 1)
	cc.Deduct(P, 2*smtLevels)
	cc.moveBalances(smtLevels)

	if isDeploy || isCreate || isCreate2 {
		cc.Deduct(S, 15)
		cc.Deduct(B, 2)
		cc.Deduct(P, 2*smtLevels)
		cc.checkBytecodeStartsEF()
		cc.hashPoseidonLinearFromMemory(bytecodeLength)
		if isCreate {
			cc.Deduct(S, 40)
			cc.Deduct(K, 1)
			cc.maskAddress()
		} else if isCreate2 {
			cc.Deduct(S, 40)
			cc.divArith()
			cc.Deduct(K, int(math.Ceil(float64(bytecodeLength+1)/136)+1))
			cc.multiCall(cc.mLoad32, bytecodeLength>>5) //int(math.Floor(float64(bytecodeLength)/32))
			cc.mLoadX()
			cc.SHRarith()
			cc.Deduct(K, 1)
			cc.maskAddress()
		}
	} else {
		cc.Deduct(P, int(math.Ceil(float64(bytecodeLength+1)/56)))
		cc.Deduct(D, int(math.Ceil(float64(bytecodeLength+1)/56)))
		if bytecodeLength >= 56 {
			cc.divArith()
		}
	}
}

func (cc *CounterCollector) moveBalances(smtLevels int) {
	cc.Deduct(S, 25)
	cc.Deduct(B, 3+2)
	cc.Deduct(P, 4*smtLevels)
}

func (cc *CounterCollector) checkBytecodeStartsEF() {
	cc.Deduct(S, 20)
	cc.mLoadX()
	cc.SHRarith()
}

func (cc *CounterCollector) hashPoseidonLinearFromMemory(memSize int) {
	cc.Deduct(S, 50)
	cc.Deduct(B, 1+1)
	cc.Deduct(P, int(math.Ceil(float64(memSize+1)/56)))
	cc.Deduct(D, int(math.Ceil(float64(memSize+1)/56)))
	cc.divArith()
	cc.multiCall(cc.hashPoseidonLinearFromMemoryLoop, memSize>>5) //int(math.Floor(float64(memSize)/32))
	cc.mLoadX()
	cc.SHRarith()
}

func (cc *CounterCollector) hashPoseidonLinearFromMemoryLoop() {
	cc.Deduct(S, 8)
	cc.mLoad32()
}

func (cc *CounterCollector) mLoad32() {
	cc.Deduct(S, 40)
	cc.Deduct(B, 2)
	cc.Deduct(M, 1)
	cc.offsetUtil()
	cc.SHRarith()
	cc.SHLarith()
}

func (cc *CounterCollector) maskAddress() {
	cc.Deduct(S, 6)
	cc.Deduct(B, 1)
}

func (cc *CounterCollector) processChangeL2Block(verifyMerkleProof bool) {
	cc.Deduct(S, 70)
	cc.Deduct(B, 4+4)
	cc.Deduct(P, 6*cc.smtLevels)
	cc.Deduct(K, 2)
	cc.consolidateBlock()
	cc.setupNewBlockInfoTree()

	if verifyMerkleProof {
		cc.verifyMerkleProof()
	}
}

func (cc *CounterCollector) setupNewBlockInfoTree() {
	cc.Deduct(S, 40)
	cc.Deduct(B, 7)
	cc.Deduct(P, 6*MCPL)
}

func (cc *CounterCollector) verifyMerkleProof() {
	cc.Deduct(S, 250)
	cc.Deduct(B, 1)
	cc.Deduct(K, 33)
}

func (cc *CounterCollector) preEcRecover(v, r, s *uint256.Int) error {
	cc.Deduct(S, 35)
	cc.Deduct(B, 1)
	cc.multiCall(cc.readFromCallDataOffset, 4)
	if err := cc.ecRecover(v, r, s, true); err != nil {
		return err
	}
	cc.mStore32()
	cc.mStoreX()

	return nil
}

func (cc *CounterCollector) preECAdd() {
	cc.Deduct(S, 50)
	cc.Deduct(B, 1)
	cc.multiCall(cc.readFromCallDataOffset, 4)
	cc.multiCall(cc.mStore32, 4)
	cc.mStoreX()
	cc.ecAdd()
}

func (cc *CounterCollector) readFromCallDataOffset() {
	cc.Deduct(S, 25)
	cc.mLoadX()
}

func (cc *CounterCollector) mStore32() {
	cc.Deduct(S, 80)
	cc.Deduct(B, 1)
	cc.Deduct(M, 1)
	cc.offsetUtil()
	cc.multiCall(cc.SHRarith, 2)
	cc.multiCall(cc.SHLarith, 2)
}

func (cc *CounterCollector) mStoreX() {
	cc.Deduct(S, 80)
	cc.Deduct(B, 1)
	cc.Deduct(M, 1)
	cc.offsetUtil()
	cc.multiCall(cc.SHRarith, 2)
	cc.multiCall(cc.SHLarith, 2)
}

func (cc *CounterCollector) decodeChangeL2BlockTx() {
	cc.Deduct(S, 20)
	cc.multiCall(cc.addBatchHashData, 3)
}

func (cc *CounterCollector) ecAdd() {
	cc.Deduct(S, 323)
	cc.Deduct(B, 33)
	cc.Deduct(A, 40)
}

func (cc *CounterCollector) preECMul() {
	cc.Deduct(S, 50)
	cc.Deduct(B, 1)
	cc.multiCall(cc.readFromCallDataOffset, 3)
	cc.multiCall(cc.mStore32, 4)
	cc.mStoreX()
	cc.ecMul()
}

func (cc *CounterCollector) ecMul() {
	cc.Deduct(S, 162890)
	cc.Deduct(B, 16395)
	cc.Deduct(A, 19161)
}

func (cc *CounterCollector) preECPairing(inputsCount int) {
	cc.Deduct(S, 50)
	cc.Deduct(B, 1)
	cc.multiCall(cc.readFromCallDataOffset, 6)
	cc.divArith()
	cc.mStore32()
	cc.mStoreX()
	cc.ecPairing(inputsCount)
}

func (cc *CounterCollector) ecPairing(inputsCount int) {
	cc.Deduct(S, 16+inputsCount*184017+171253)
	cc.Deduct(B, inputsCount*3986+650)
	cc.Deduct(A, inputsCount*13694+15411)
}

func (cc *CounterCollector) preModExp(callDataLength, returnDataLength, bLen, mLen, eLen int, base, exponent, modulus *big.Int) {
	cc.Deduct(S, 100)
	cc.Deduct(B, 20)
	cc.multiCall(cc.readFromCallDataOffset, 4)
	cc.SHRarith()
	cc.multiCall(cc.addArith, 2)
	cc.multiCall(cc.divArith, 3)
	cc.multiCall(cc.mulArith, 3)
	cc.subArith()
	cc.multiCall(cc.SHLarith, 2)
	cc.multiCall(cc.mStoreX, 2)
	cc.multiCall(cc.preModExpLoop, callDataLength>>5)   //int(math.Floor(float64(memSize)/32))
	cc.multiCall(cc.preModExpLoop, returnDataLength>>5) //int(math.Floor(float64(memSize)/32))
	if modulus.Uint64() > 0 {
		cc.modExp(bLen, mLen, eLen, base, exponent, modulus)
	}
}

func (cc *CounterCollector) modExp(bLen, mLen, eLen int, base, exponent, modulus *big.Int) {
	steps, binary, arith := expectedModExpCounters(
		int(math.Ceil(float64(bLen)/32)),
		int(math.Ceil(float64(mLen)/32)),
		int(math.Ceil(float64(eLen)/32)),
		base,
		exponent,
		modulus,
	)
	cc.Deduct(S, int(steps.Int64()))
	cc.Deduct(B, int(binary.Int64()))
	cc.Deduct(A, int(arith.Int64()))
}

func (cc *CounterCollector) preModExpLoop() {
	cc.Deduct(S, 8)
	cc.mStore32()
}

func (cc *CounterCollector) multiCall(call func(), times int) {
	for i := 0; i < times; i++ {
		// if there is a case with a huge amount ot iterations
		// it will overflow after several thousand iterations
		// so we can just stop it early and not hang the node
		// check each 1000 itearations so the overhead is not too much in the normal case
		if i%1000 == 0 {
			if cc.isOverflown() {
				break
			}
		}
		call()
	}
}

func (cc *CounterCollector) preSha256(callDataLength int) {
	cc.Deduct(S, 100)
	cc.Deduct(B, 1)
	cc.Deduct(SHA, int(math.Ceil(float64(callDataLength+8)/64)))
	cc.multiCall(cc.divArith, 2)
	cc.mStore32()
	cc.mStoreX()
	cc.multiCall(cc.preSha256Loop, callDataLength>>5) //int(math.Floor(float64(callDataLength)/32))
	cc.readFromCallDataOffset()
	cc.SHRarith()
}

func (cc *CounterCollector) preSha256Loop() {
	cc.Deduct(S, 11)
	cc.readFromCallDataOffset()
}

func (cc *CounterCollector) preIdentity(callDataLength, returnDataLength int) {
	cc.Deduct(S, 45)
	cc.Deduct(B, 2)
	cc.divArith()
	// identity loop
	cc.multiCall(cc.identityLoop, callDataLength>>5) //int(math.Floor(float64(callDataLength)/32))
	cc.readFromCallDataOffset()
	cc.mStoreX()
	// identity return loop
	cc.multiCall(cc.identityReturnLoop, returnDataLength>>5)
	cc.mLoadX()
	cc.mStoreX()
}

func (cc *CounterCollector) identityLoop() {
	cc.Deduct(S, 8)
	cc.readFromCallDataOffset()
	cc.mStore32()
}

func (cc *CounterCollector) identityReturnLoop() {
	cc.Deduct(S, 12)
	cc.mLoad32()
	cc.mStore32()
}

func (cc *CounterCollector) abs() {
	cc.Deduct(S, 10)
	cc.Deduct(B, 2)
}

func (cc *CounterCollector) opAdd(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opMul(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.mulArith()
	return nil, nil
}

func (cc *CounterCollector) opSub(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opDiv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 15)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opSDiv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 25)
	cc.Deduct(B, 1)
	cc.multiCall(cc.abs, 2)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 20)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opSMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 20)
	cc.Deduct(B, 1)
	cc.multiCall(cc.abs, 2)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opAddMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 30)
	cc.Deduct(B, 3)
	cc.Deduct(A, 1)
	return nil, nil
}

func (cc *CounterCollector) opMulMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.utilMulMod()
	return nil, nil
}

func (cc *CounterCollector) utilMulMod() {
	cc.Deduct(S, 50)
	cc.Deduct(B, 4)
	cc.Deduct(A, 2)
	cc.mulArith()
}

func (cc *CounterCollector) opExp(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)

	var exponentLength int
	exponent := scope.Stack.PeekAt(2)
	if exponent.IsZero() {
		exponentLength = 1
	} else {
		exponentLength = len(exponent.Bytes())
	}

	cc.getLenBytes(exponentLength)
	cc.expAd(exponentLength * 8)
	return nil, nil
}

func (cc *CounterCollector) expAd(inputLength int) {
	cc.Deduct(S, 30)
	cc.Deduct(B, 2)
	cc.getLenBits(inputLength)
	for i := 0; i < inputLength; i++ {
		cc.Deduct(S, 12)
		cc.Deduct(B, 2)
		cc.divArith()
		cc.mulArith()
		cc.mulArith()
	}
}

func (cc *CounterCollector) getLenBits(inputLength int) {
	cc.Deduct(S, 12)
	for i := 0; i < inputLength; i++ {
		cc.Deduct(S, 9)
		cc.Deduct(B, 1)
		cc.divArith()
	}
}

func (cc *CounterCollector) opSignExtend(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 20)
	cc.Deduct(B, 6)
	cc.Deduct(P, 2*cc.smtLevels)
	return nil, nil
}

func (cc *CounterCollector) opBlockHash(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 15)
	cc.Deduct(P, cc.smtLevels)
	cc.Deduct(K, 1)
	return nil, nil
}

func (cc *CounterCollector) opCoinbase(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opTimestamp(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opNumber(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opDifficulty(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opGasLimit(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opChainId(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opCalldataLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 15)
	cc.readFromCallDataOffset()
	return nil, nil
}

func (cc *CounterCollector) opCalldataSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	return nil, nil
}

func (cc *CounterCollector) opCalldataCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	inputLen := int(scope.Stack.PeekAt(3).Uint64())
	if inputLen != 0 {
		cc.opCode(scope)
		cc.Deduct(S, 100)
		cc.Deduct(B, 2)
		cc.saveMem(inputLen)
		cc.offsetUtil()
		cc.multiCall(cc.opCalldataCopyLoop, inputLen>>5) //int(math.Floor(float64(inputLen)/32))
		cc.readFromCallDataOffset()
		cc.multiCall(cc.mStoreX, 2)
	}
	return nil, nil
}

func (cc *CounterCollector) saveMem(inputSize int) {
	if inputSize == 0 {
		cc.Deduct(S, 12)
		cc.Deduct(B, 1)
		return
	}
	cc.Deduct(S, 50)
	cc.Deduct(B, 5)
	cc.mulArith()
	cc.divArith()
}

func (cc *CounterCollector) opCalldataCopyLoop() {
	cc.Deduct(S, 30)
	cc.readFromCallDataOffset()
	cc.offsetUtil()
	cc.Deduct(M, 1)
}

func (cc *CounterCollector) opCodeSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(P, cc.smtLevels)
	return nil, nil
}

func (cc *CounterCollector) opExtCodeSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(P, cc.smtLevels)
	cc.maskAddress()
	cc.isColdAddress()
	return nil, nil
}

func (cc *CounterCollector) opExtCodeCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	stack := scope.Stack
	address := stack.PeekAt(1).Bytes20()
	bytecodeLen := interpreter.evm.IntraBlockState().GetCodeSize(address)
	length := int(stack.PeekAt(4).Uint64()) // no need to read contract storage as we only care about the byte length
	if length != 0 {
		cc.opCode(scope)
		cc.Deduct(S, 60)
		cc.maskAddress()
		cc.isColdAddress()
		cc.Deduct(P, 2*cc.smtLevels+int(math.Ceil(float64(bytecodeLen)/56)))
		cc.Deduct(D, int(math.Ceil(float64(bytecodeLen)/56)))
		cc.multiCall(cc.divArith, 2)
		cc.saveMem(length)
		cc.mulArith()
		cc.Deduct(M, length)
		cc.multiCall(cc.opCodeCopyLoop, length)
		cc.Deduct(B, 1)
	}
	return nil, nil
}

func (cc *CounterCollector) opCodeCopyLoop() {
	cc.Deduct(S, 30)
	cc.Deduct(B, 2)
	cc.Deduct(M, 1)
	cc.offsetUtil()
}

func (cc *CounterCollector) opCodeCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	length := int(scope.Stack.PeekAt(3).Uint64())
	if length != 0 {
		cc.opCode(scope)
		if scope.Contract.IsCreate || cc.isDeploy {
			_, err := cc.opCalldataCopy(pc, interpreter, scope)
			if err != nil {
				return nil, err
			}
		} else {
			cc.Deduct(S, 40)
			cc.Deduct(B, 3)
			cc.saveMem(length)
			cc.divArith()
			cc.mulArith()
			cc.multiCall(cc.opCodeCopyLoop, length)
		}
	}
	return nil, nil
}

func (cc *CounterCollector) opReturnDataSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 11)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opReturnDataCopy(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	length := int(scope.Stack.PeekAt(3).Uint64())
	if length != 0 {
		cc.opCode(scope)
		cc.Deduct(S, 50)
		cc.Deduct(B, 2)
		cc.saveMem(length)
		cc.divArith()
		cc.mulArith()
		cc.multiCall(cc.returnDataCopyLoop, length>>5) //int(math.Floor(float64(length)/32))
		cc.mLoadX()
		cc.mStoreX()
	}
	return nil, nil
}

func (cc *CounterCollector) returnDataCopyLoop() {
	cc.Deduct(S, 10)
	cc.mLoad32()
	cc.mStore32()
}

func (cc *CounterCollector) opExtCodeHash(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(P, cc.smtLevels)
	cc.maskAddress()
	cc.isColdAddress()
	return nil, nil
}

func (cc *CounterCollector) opLT(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opGT(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opSLT(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opSGT(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opEQ(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opIsZero(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opAnd(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opOr(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}
func (cc *CounterCollector) opXor(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}
func (cc *CounterCollector) opNot(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}
func (cc *CounterCollector) opByte(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 15)
	cc.Deduct(B, 2)
	cc.SHRarith()
	return nil, nil
}
func (cc *CounterCollector) opSHR(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(B, 1)
	cc.shrArithBit()
	return nil, nil
}

func (cc *CounterCollector) shrArithBit() {
	cc.Deduct(S, 30)
	cc.Deduct(B, 2)
	cc.divArith()
}

func (cc *CounterCollector) opSHL(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(B, 1)
	cc.shlArithBit()
	return nil, nil
}

func (cc *CounterCollector) shlArithBit() {
	cc.Deduct(S, 50)
	cc.Deduct(B, 2)
	cc.Deduct(A, 2)
}

func (cc *CounterCollector) opSAR(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 25)
	cc.Deduct(B, 5)
	cc.shrArithBit()
	return nil, nil
}

func (cc *CounterCollector) opStop(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 20)
	return nil, nil
}

func (cc *CounterCollector) opCreate(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	size := int(scope.Stack.PeekAt(3).Uint64())
	nonce := interpreter.evm.IntraBlockState().GetNonce(scope.Contract.CallerAddress) + 1
	nonceByteLength := getRelevantNumberBytes(hermez_db.Uint64ToBytes(nonce))
	cc.opCode(scope)
	cc.Deduct(S, 70)
	cc.Deduct(B, 3)
	cc.Deduct(P, 3*cc.smtLevels)
	cc.saveMem(size)
	cc.getLenBytes(nonceByteLength)
	cc.computeGasSendCall()
	cc.saveCalldataPointer()
	cc.checkpointBlockInfoTree()
	cc.checkpointTouched()

	cc.processContractCall(cc.smtLevels, size, false, true, false)

	return nil, nil
}

func (cc *CounterCollector) computeGasSendCall() {
	cc.Deduct(S, 25)
	cc.Deduct(B, 2)
}

func (cc *CounterCollector) saveCalldataPointer() {
	cc.Deduct(S, 6)
}

func (cc *CounterCollector) checkpointBlockInfoTree() {
	cc.Deduct(S, 4)
}

func (cc *CounterCollector) checkpointTouched() {
	cc.Deduct(S, 2)
}

func (cc *CounterCollector) opCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	addr := scope.Stack.PeekAt(2)
	toAddrBytecodeLength := interpreter.evm.IntraBlockState().GetCodeSize(addr.Bytes20())
	inSize := int(scope.Stack.PeekAt(5).Uint64())
	outSize := int(scope.Stack.PeekAt(7).Uint64())

	cc.opCode(scope)
	cc.Deduct(S, 80)
	cc.Deduct(B, 5)
	cc.maskAddress()
	cc.saveMem(inSize)
	cc.saveMem(outSize)
	cc.isColdAddress()
	cc.isEmptyAccount()
	cc.computeGasSendCall()
	cc.saveCalldataPointer()
	cc.checkpointBlockInfoTree()
	cc.checkpointTouched()

	cc.processContractCall(cc.smtLevels, toAddrBytecodeLength, false, false, false)

	return nil, nil
}

func (cc *CounterCollector) isEmptyAccount() {
	cc.Deduct(S, 30)
	cc.Deduct(B, 3)
	cc.Deduct(P, 3*cc.smtLevels)
}

func (cc *CounterCollector) opCallCode(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	addr := scope.Stack.PeekAt(2)
	toAddrBytecodeLength := interpreter.evm.IntraBlockState().GetCodeSize(addr.Bytes20())
	inSize := int(scope.Stack.PeekAt(5).Uint64())
	outSize := int(scope.Stack.PeekAt(7).Uint64())

	cc.opCode(scope)
	cc.Deduct(S, 80)
	cc.Deduct(B, 5)
	cc.maskAddress()
	cc.saveMem(inSize)
	cc.saveMem(outSize)
	cc.isColdAddress()
	cc.computeGasSendCall()
	cc.saveCalldataPointer()
	cc.checkpointBlockInfoTree()
	cc.checkpointTouched()

	cc.processContractCall(cc.smtLevels, toAddrBytecodeLength, false, false, false)

	return nil, nil
}

func (cc *CounterCollector) opDelegateCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	addr := scope.Stack.PeekAt(2)
	toAddrBytecodeLength := interpreter.evm.IntraBlockState().GetCodeSize(addr.Bytes20())
	inSize := int(scope.Stack.PeekAt(4).Uint64())
	outSize := int(scope.Stack.PeekAt(6).Uint64())

	cc.opCode(scope)
	cc.Deduct(S, 80)
	cc.maskAddress()
	cc.saveMem(inSize)
	cc.saveMem(outSize)
	cc.isColdAddress()
	cc.computeGasSendCall()
	cc.saveCalldataPointer()
	cc.checkpointBlockInfoTree()
	cc.checkpointTouched()

	cc.processContractCall(cc.smtLevels, toAddrBytecodeLength, false, false, false)

	return nil, nil
}

func (cc *CounterCollector) opStaticCall(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	addr := scope.Stack.PeekAt(2)
	toAddrBytecodeLength := interpreter.evm.IntraBlockState().GetCodeSize(addr.Bytes20())
	inSize := int(scope.Stack.PeekAt(4).Uint64())
	outSize := int(scope.Stack.PeekAt(6).Uint64())

	cc.opCode(scope)
	cc.Deduct(S, 80)
	cc.maskAddress()
	cc.saveMem(inSize)
	cc.saveMem(outSize)
	cc.isColdAddress()
	cc.computeGasSendCall()
	cc.saveCalldataPointer()
	cc.checkpointBlockInfoTree()
	cc.checkpointTouched()

	cc.processContractCall(cc.smtLevels, toAddrBytecodeLength, false, false, false)

	return nil, nil
}

func (cc *CounterCollector) opCreate2(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	inSize := int(scope.Stack.PeekAt(3).Uint64())
	accNonce := interpreter.evm.IntraBlockState().GetNonce(scope.Contract.CallerAddress) + 1
	nonceByteLength := getRelevantNumberBytes(hermez_db.Uint64ToBytes(accNonce))
	cc.opCode(scope)
	cc.Deduct(S, 80)
	cc.Deduct(B, 4)
	cc.Deduct(P, 2*cc.smtLevels)
	cc.saveMem(inSize)
	cc.divArith()
	cc.getLenBytes(nonceByteLength)
	cc.computeGasSendCall()
	cc.saveCalldataPointer()
	cc.checkpointBlockInfoTree()
	cc.checkpointTouched()

	cc.processContractCall(cc.smtLevels, inSize, false, false, true)

	return nil, nil
}

func (cc *CounterCollector) opReturn(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	returnSize := int(scope.Stack.PeekAt(2).Uint64())
	cc.opCode(scope)
	cc.Deduct(S, 30)
	cc.Deduct(B, 1)
	cc.saveMem(returnSize)
	if cc.isDeploy || scope.Contract.IsCreate {
		if scope.Contract.IsCreate {
			cc.Deduct(S, 25)
			cc.Deduct(B, 2)
			cc.Deduct(P, 2*cc.smtLevels)
			cc.checkBytecodeStartsEF()
			cc.hashPoseidonLinearFromMemory(returnSize)
		}
	} else {
		cc.multiCall(cc.returnLoop, returnSize>>5) //int(math.Floor(float64(returnSize)/32))
		cc.mLoadX()
		cc.mStoreX()
	}
	return nil, nil
}

func (cc *CounterCollector) returnLoop() {
	cc.Deduct(S, 12)
	cc.mLoad32()
	cc.mStore32()
}

func (cc *CounterCollector) opRevert(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	size := int(scope.Stack.PeekAt(2).Uint64())
	cc.opCode(scope)
	cc.Deduct(S, 40)
	cc.Deduct(B, 1)
	cc.revertTouched()
	cc.revertBlockInfoTree()
	cc.saveMem(size)
	cc.multiCall(cc.revertLoop, size>>5) //int(math.Floor(float64(size)/32))
	cc.mLoadX()
	cc.mStoreX()
	return nil, nil
}

func (cc *CounterCollector) revertTouched() {
	cc.Deduct(S, 2)
}

func (cc *CounterCollector) revertBlockInfoTree() {
	cc.Deduct(S, 4)
}

func (cc *CounterCollector) revertLoop() {
	cc.Deduct(S, 12)
	cc.mLoad32()
	cc.mStore32()
}

func (cc *CounterCollector) opSendAll(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 60)
	cc.Deduct(B, 2+1)
	cc.Deduct(P, 4*cc.smtLevels)
	cc.maskAddress()
	cc.isEmptyAccount()
	cc.isColdAddress()
	cc.addArith()
	return nil, nil
}

func (cc *CounterCollector) opInvalid(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 50)
	return nil, nil
}

func (cc *CounterCollector) opAddress(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 6)
	return nil, nil
}

func (cc *CounterCollector) opSelfBalance(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(P, cc.smtLevels)
	return nil, nil
}

func (cc *CounterCollector) opBalance(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.Deduct(P, cc.smtLevels)
	cc.maskAddress()
	cc.isColdAddress()
	return nil, nil
}

func (cc *CounterCollector) opOrigin(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opCaller(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opCallValue(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opGasPrice(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	return nil, nil
}

func (cc *CounterCollector) opGas(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 4)
	return nil, nil
}

func (cc *CounterCollector) opSha3(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	size := int(scope.Stack.PeekAt(2).Uint64())
	cc.opCode(scope)
	cc.Deduct(S, 40)
	cc.Deduct(K, int(math.Ceil(float64(size+1)/136)))
	cc.saveMem(size)
	cc.multiCall(cc.divArith, 2)
	cc.mulArith()
	cc.multiCall(cc.sha3Loop, size>>5) //Math.floor(input.inputSize / 32)
	cc.mLoadX()
	cc.SHRarith()
	return nil, nil
}

func (cc *CounterCollector) sha3Loop() {
	cc.Deduct(S, 8)
	cc.mLoad32()
}

func (cc *CounterCollector) opJump(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 5)
	cc.checkJumpDest(scope)
	return nil, nil
}

func (cc *CounterCollector) checkJumpDest(scope *ScopeContext) {
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	if scope.Contract.IsCreate {
		if cc.isDeploy {
			cc.Deduct(B, 1)
			cc.mLoadX()
		}
	}
}

func (cc *CounterCollector) opJumpI(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	cc.checkJumpDest(scope)
	return nil, nil
}

func (cc *CounterCollector) opPC(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 4)
	return nil, nil
}

func (cc *CounterCollector) opJumpDest(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 2)
	return nil, nil
}

func (cc *CounterCollector) log(scope *ScopeContext) {
	size := int(scope.Stack.PeekAt(2).Uint64())
	cc.opCode(scope)
	cc.Deduct(S, 34+7*4)
	cc.saveMem(size)
	cc.mulArith()
	cc.divArith()
	cc.Deduct(P, int(math.Ceil(float64(size)/56)+4))
	cc.Deduct(D, int(math.Ceil(float64(size)/56)+4))
	cc.multiCall(cc.logLoop, (size+1)>>5) //Math.floor((input.inputSize + 1) / 32))
	cc.mLoadX()
	cc.SHRarith()
	cc.fillBlockInfoTreeWithLog()
	cc.Deduct(B, 1)
}

func (cc *CounterCollector) logLoop() {
	cc.Deduct(S, 10)
	cc.mLoad32()
}

func (cc *CounterCollector) fillBlockInfoTreeWithLog() {
	cc.Deduct(S, 11)
	cc.Deduct(P, MCPL)
	cc.Deduct(B, 1)
}

func (cc *CounterCollector) opLog0(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.log(scope)
	return nil, nil
}

func (cc *CounterCollector) opLog1(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.log(scope)
	return nil, nil
}

func (cc *CounterCollector) opLog2(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.log(scope)
	return nil, nil
}

func (cc *CounterCollector) opLog3(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.log(scope)
	return nil, nil
}

func (cc *CounterCollector) opLog4(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.log(scope)
	return nil, nil
}

func (cc *CounterCollector) opPush0(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 4)
	return nil, nil
}

func (cc *CounterCollector) opPushGenerator(num int) executionFunc {
	return func(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
		cc.opPush(num, scope)
		return nil, nil
	}
}

func (cc *CounterCollector) opPush(num int, scope *ScopeContext) {
	cc.opCode(scope)
	cc.Deduct(S, 2)
	if scope.Contract.IsCreate || cc.isDeploy {
		if scope.Contract.IsCreate {
			cc.Deduct(S, 20)
			cc.mLoadX()
			cc.SHRarith()
		} else {
			cc.Deduct(S, 10)
			for i := 0; i < num; i++ {
				cc.Deduct(S, 10)
				cc.SHLarith()
			}
		}
	} else {
		cc.Deduct(S, 10)
		cc.readPush(num)
	}
}

func (cc *CounterCollector) readPush(num int) {
	switch num {
	case 1:
		cc.Deduct(S, 2)
	case 2:
		cc.Deduct(S, 4)
	case 3:
		cc.Deduct(S, 5)
	case 4:
		cc.Deduct(S, 6)
	case 32:
		cc.Deduct(S, 45)
	default:
		cc.Deduct(S, 6+num*2) // approx value, is a bit less
	}
}

func (cc *CounterCollector) opDup(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 6)
	return nil, nil
}

func (cc *CounterCollector) opSwap(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 7)
	return nil, nil
}

func (cc *CounterCollector) opPop(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 3)
	return nil, nil
}

func (cc *CounterCollector) opMLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 8)
	cc.saveMem(32)
	cc.mLoad32()
	return nil, nil
}

func (cc *CounterCollector) opMStore(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 22)
	cc.Deduct(M, 1)
	cc.saveMem(32)
	cc.offsetUtil()
	return nil, nil
}

func (cc *CounterCollector) opMStore8(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 13)
	cc.Deduct(M, 1)
	cc.saveMem(1)
	cc.offsetUtil()
	return nil, nil
}

func (cc *CounterCollector) opMSize(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 15)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opSLoad(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 10)
	cc.Deduct(P, cc.smtLevels)
	cc.isColdSlot()
	return nil, nil
}

func (cc *CounterCollector) isColdSlot() {
	cc.Deduct(S, 20)
	cc.Deduct(B, 1)
	cc.Deduct(P, 2*MCPL)
}

func (cc *CounterCollector) opSSTore(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope)
	cc.Deduct(S, 70)
	cc.Deduct(B, 8)
	cc.Deduct(P, 3*cc.smtLevels)
	cc.isColdSlot()
	return nil, nil
}

func getRelevantNumberBytes(input []byte) int {
	totalLength := len(input)
	rel := 0
	for i := 0; i < totalLength; i++ {
		if input[i] == 0 {
			continue
		}
		rel = i
		break
	}
	return totalLength - rel
}
