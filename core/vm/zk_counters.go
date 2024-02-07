package vm

import (
	"errors"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types"
	"math"
	"math/big"
	"strconv"
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
	S   CounterKey = "S"
	A   CounterKey = "A"
	B   CounterKey = "B"
	M   CounterKey = "M"
	K   CounterKey = "K"
	D   CounterKey = "D"
	P   CounterKey = "P"
	SHA CounterKey = "SHA"
)

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
	counters  Counters
	smtLevels int
}

func calculateSmtLevels(smtMaxLevel uint32) int {
	return len(strconv.FormatInt(int64(math.Pow(2, float64(smtMaxLevel))+250000), 2))
}

func NewCounterCollector(smtLevels int) *CounterCollector {
	return &CounterCollector{
		counters:  defaultCounters(),
		smtLevels: smtLevels,
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
		SHA: {
			remaining:     int(math.Floor(totalSteps-1)/31488) * 7,
			name:          "sha256",
			initialAmount: int(math.Floor(totalSteps-1)/31488) * 7,
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
		ADD:        cc.opAdd,
		MUL:        cc.opMul,
		SUB:        cc.opSub,
		DIV:        cc.opDiv,
		SDIV:       cc.opSDiv,
		MOD:        cc.opMod,
		SMOD:       cc.opSMod,
		ADDMOD:     cc.opAddMod,
		MULMOD:     cc.opMulMod,
		EXP:        cc.opExp,
		SIGNEXTEND: cc.opSignExtend,
		BLOCKHASH:  cc.opBlockHash,
	}
	return calls
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
	cc.Deduct(S, 10)
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
	if r.Uint64() == 0 || fnecMinusOne.Lt(r) || s.Uint64() == 0 || upperLimit.Lt(s) || (v.Uint64() != 27 && v.Uint64() != 28) {
		cc.Deduct(S, 45)
		cc.Deduct(A, 2)
		cc.Deduct(B, 8)
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
	cc.Deduct(S, 50)
	cc.Deduct(B, 1)
	cc.Deduct(A, 1)
}

func (cc *CounterCollector) fillBlockInfoTreeWithTxReceipt(smtLevels int) {
	cc.Deduct(S, 20)
	cc.Deduct(P, 3*smtLevels)
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
		} else if isCreate2 {
			cc.Deduct(S, 40)
			cc.divArith()
			cc.Deduct(K, int(math.Ceil(float64(bytecodeLength+1)/136)+1))
			cc.multiCall(cc.mLoad32, int(math.Floor(float64(bytecodeLength)/32)))
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
	cc.Deduct(P, int(math.Ceil(float64(memSize+1))/56))
	cc.Deduct(D, int(math.Ceil(float64(memSize+1))/56))
	cc.divArith()
	cc.multiCall(cc.hashPoseidonLinearFromMemoryLoop, int(math.Floor(float64(memSize)/32)))
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

func (cc *CounterCollector) processChangeL2Block() {
	cc.Deduct(S, 70)
	cc.Deduct(B, 4+4)
	cc.Deduct(P, 6*cc.smtLevels)
	cc.Deduct(K, 2)
	cc.consolidateBlock()
	cc.setupNewBlockInfoTree()
	cc.verifyMerkleProof()
}

func (cc *CounterCollector) setupNewBlockInfoTree() {
	cc.Deduct(S, 40)
	cc.Deduct(B, 7)
	cc.Deduct(P, 6*MCPL)
}

func (cc *CounterCollector) verifyMerkleProof() {
	cc.Deduct(S, 250)
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
	cc.Deduct(S, 100)
	cc.Deduct(B, 1)
	cc.Deduct(M, 1)
	cc.offsetUtil()
	cc.multiCall(cc.SHRarith, 2)
	cc.multiCall(cc.SHLarith, 2)
}

func (cc *CounterCollector) mStoreX() {
	cc.Deduct(S, 100)
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
	cc.multiCall(cc.preModExpLoop, int(math.Floor(float64(callDataLength)/32)))
	cc.multiCall(cc.preModExpLoop, int(math.Floor(float64(returnDataLength)/32)))
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
		call()
	}
}

func (cc *CounterCollector) preSha256(callDataLength uint64) {
	cc.Deduct(S, 100)
	cc.Deduct(B, 1)
	cc.Deduct(SHA, int(math.Ceil(float64(callDataLength+1)/64)))
	cc.multiCall(cc.divArith, 2)
	cc.mStore32()
	cc.mStoreX()
	cc.multiCall(cc.preSha256Loop, int(math.Floor(float64(callDataLength)/32)))
	cc.readFromCallDataOffset()
	cc.SHRarith()
}

func (cc *CounterCollector) preSha256Loop() {
	cc.Deduct(S, 11)
	cc.readFromCallDataOffset()
}

func (cc *CounterCollector) preIdentity(callDataLength, returnDataLength uint64) {
	cc.Deduct(S, 45)
	cc.Deduct(B, 2)
	cc.divArith()
	// identity loop
	cc.multiCall(cc.identityLoop, int(math.Floor(float64(callDataLength)/32)))
	cc.readFromCallDataOffset()
	cc.mStoreX()
	// identity return loop
	cc.multiCall(cc.identityReturnLoop, int(math.Floor(float64(returnDataLength)/32)))
	cc.mLoadX()
	cc.mStoreX()
}

func (cc *CounterCollector) identityLoop() {
	cc.Deduct(S, 8)
	cc.readFromCallDataOffset()
	cc.mStore32()
}

func (cc *CounterCollector) identityReturnLoop() {
	cc.Deduct(S, 8)
	cc.readFromCallDataOffset()
	cc.mStore32()
}

func (cc *CounterCollector) abs() {
	cc.Deduct(S, 10)
	cc.Deduct(B, 2)
}

func (cc *CounterCollector) opAdd(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opMul(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 10)
	cc.mulArith()
	return nil, nil
}

func (cc *CounterCollector) opSub(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 10)
	cc.Deduct(B, 1)
	return nil, nil
}

func (cc *CounterCollector) opDiv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 15)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opSDiv(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 25)
	cc.Deduct(B, 1)
	cc.multiCall(cc.abs, 2)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 20)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opSMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 20)
	cc.Deduct(B, 1)
	cc.multiCall(cc.abs, 2)
	cc.divArith()
	return nil, nil
}

func (cc *CounterCollector) opAddMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 30)
	cc.Deduct(B, 3)
	cc.Deduct(A, 1)
	return nil, nil
}

func (cc *CounterCollector) opMulMod(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
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
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 10)
	exponent := scope.Stack.Peek()
	exponentLength := len(exponent.Bytes())
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
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 20)
	cc.Deduct(B, 6)
	cc.Deduct(P, 2*cc.smtLevels)
	return nil, nil
}

func (cc *CounterCollector) opBlockHash(pc *uint64, interpreter *EVMInterpreter, scope *ScopeContext) ([]byte, error) {
	cc.opCode(scope.Contract.IsCreate)
	cc.Deduct(S, 20)
	cc.Deduct(B, 6)
	cc.Deduct(P, 2*cc.smtLevels)
	return nil, nil
}
