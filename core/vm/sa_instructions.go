package vm

import (
	"github.com/holiman/uint256"
	//"golang.org/x/crypto/sha3"

	//"github.com/ledgerwatch/turbo-geth/common"
	//"github.com/ledgerwatch/turbo-geth/core/types"
	//"github.com/ledgerwatch/turbo-geth/params"
)

func saStop(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return nil, nil
}

func saUnaryOp(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx, op func(*uint256.Int)) ([]byte, error) {
	x, xkind := callContext.stack.peek()
	if xkind == Constant {
		op(x) // x already has the right kind
		return nil, nil
	}
	if xkind == Dynamic {
		callContext.stack.setKind(0, Dynamic)
		return nil, nil
	}
	if xkind == Unified {
		callContext.stack.setKind(0, Unified)
		return nil, nil
	}
	callContext.stack.setKind(0, Input)
	return nil, nil	
}

func saBinaryOp(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx, op func(*uint256.Int, *uint256.Int)) ([]byte, error) {
	x, xkind := callContext.stack.pop()
	y, ykind := callContext.stack.peek()
	if xkind == Constant && ykind == Constant {
		op(&x, y) // y already has the right kind
		return nil, nil
	}
	if xkind == Dynamic || ykind == Dynamic {
		callContext.stack.setKind(0, Dynamic)
		return nil, nil
	}
	if xkind == Unified || ykind == Unified {
		callContext.stack.setKind(0, Unified)
		return nil, nil
	}
	callContext.stack.setKind(0, Input)
	return nil, nil	
}

func saTernaryOp(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx, op func(*uint256.Int, *uint256.Int, *uint256.Int)) ([]byte, error) {
	x, xkind := callContext.stack.pop()
	y, ykind := callContext.stack.pop()
	z, zkind := callContext.stack.peek()
	if xkind == Constant && ykind == Constant && zkind == Constant {
		op(&x, &y, z) // z already has the right kind
		return nil, nil
	}
	if xkind == Dynamic || ykind == Dynamic || zkind == Dynamic {
		callContext.stack.setKind(0, Dynamic)
		return nil, nil
	}
	if xkind == Unified || ykind == Unified || zkind == Unified {
		callContext.stack.setKind(0, Unified)
		return nil, nil
	}
	callContext.stack.setKind(0, Input)
	return nil, nil	
}

func saAdd(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		y.Add(x, y)
	})
}

func saMul(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		y.Mul(x, y)
	})
}

func saSub(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		y.Sub(x, y)
	})
}

func saDiv(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		y.Div(x, y)
	})
}

func saSdiv(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		y.SDiv(x, y)
	})
}

func saMod(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		y.Mod(x, y)
	})
}

func saSmod(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		y.SMod(x, y)
	})
}

func saAddmod(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saTernaryOp(pc, interpreter, callContext, func(x, y, z *uint256.Int) {
		if z.IsZero() {
			z.Clear()
		} else {
			z.AddMod(x, y, z)
		}
	})
}

func saMulmod(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saTernaryOp(pc, interpreter, callContext, func(x, y, z *uint256.Int) {
		if z.IsZero() {
			z.Clear()
		} else {
			z.MulMod(x, y, z)
		}
	})
}

func saExp(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(base, exponent *uint256.Int) {
		switch {
		case exponent.IsZero():
			// x ^ 0 == 1
			exponent.SetOne()
		case base.IsZero():
			// 0 ^ y, if y != 0, == 0
			exponent.Clear()
		case exponent.LtUint64(2): // exponent == 1
			// x ^ 1 == x
			exponent.Set(base)
		case base.LtUint64(2): // base == 1
			// 1 ^ y == 1
			exponent.SetOne()
		case base.LtUint64(3): // base == 2
			if exponent.LtUint64(256) {
				n := uint(exponent.Uint64())
				exponent.SetOne()
				exponent.Lsh(exponent, n)
			} else {
				exponent.Clear()
			}
		default:
			exponent.Exp(base, exponent)
		}
	})
}

func saSignExtend(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(back, num *uint256.Int) {
		num.ExtendSign(num, back)
	})
}

func saLt(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		if x.Lt(y) {
			y.SetOne()
		} else {
			y.Clear()
		}
	})
}

func saGt(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		if x.Gt(y) {
			y.SetOne()
		} else {
			y.Clear()
		}
	})
}

func saSlt(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		if x.Slt(y) {
			y.SetOne()
		} else {
			y.Clear()
		}
	})
}

func saSgt(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		if x.Sgt(y) {
			y.SetOne()
		} else {
			y.Clear()
		}
	})
}

func saEq(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		if x.Eq(y) {
			y.SetOne()
		} else {
			y.Clear()
		}
	})
}

func saIszero(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saUnaryOp(pc, interpreter, callContext, func(x *uint256.Int) {
		if x.IsZero() {
			x.SetOne()
		} else {
			x.Clear()
		}
	})
}

func saAnd(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	return saBinaryOp(pc, interpreter, callContext, func(x, y *uint256.Int) {
		y.And(x, y)
	})
}

// opPush1 is a specialized version of pushN
func saPush1(pc *uint64, interpreter *SaInterpreter, callContext *saCallCtx) ([]byte, error) {
	var (
		codeLen = uint64(len(callContext.contract.Code))
		integer = new(uint256.Int)
	)
	*pc++
	if *pc < codeLen {
		callContext.stack.Push(integer.SetUint64(uint64(callContext.contract.Code[*pc])))
	} else {
		callContext.stack.Push(integer)
	}
	return nil, nil
}