package params

import (
	"context"
	"math/big"
)

type configKey int

const (
	IsHomesteadEnabled configKey = iota
	IsEIP150Enabled
	IsEIP155Enabled
	IsEIP158Enabled
	IsEIP2027Enabled
	IsByzantiumEnabled
	IsConstantinopleEnabled
	IsPetersburgEnabled
	IsEWASM
	BlockNumber
	NoHistory
)

func (c *ChainConfig) WithEIPsFlags(ctx context.Context, blockNum *big.Int) context.Context {
	ctx = context.WithValue(ctx, IsHomesteadEnabled, c.IsHomestead(blockNum))
	ctx = context.WithValue(ctx, IsEIP150Enabled, c.IsEIP150(blockNum))
	ctx = context.WithValue(ctx, IsEIP155Enabled, c.IsEIP155(blockNum))
	ctx = context.WithValue(ctx, IsEIP158Enabled, c.IsEIP158(blockNum))
	ctx = context.WithValue(ctx, IsEIP2027Enabled, c.IsEIP2027(blockNum))
	ctx = context.WithValue(ctx, IsByzantiumEnabled, c.IsByzantium(blockNum))
	ctx = context.WithValue(ctx, IsConstantinopleEnabled, c.IsConstantinople(blockNum))
	ctx = context.WithValue(ctx, IsPetersburgEnabled, c.IsPetersburg(blockNum))
	ctx = context.WithValue(ctx, IsEWASM, c.IsEWASM(blockNum))
	ctx = context.WithValue(ctx, BlockNumber, blockNum)
	return ctx
}

func (c *ChainConfig) WithNoHistory(ctx context.Context, defaultValue bool, f noHistFunc) context.Context {
	return context.WithValue(ctx, NoHistory, getIsNoHistory(defaultValue, f))
}

func GetForkFlag(ctx context.Context, name configKey) bool {
	b := ctx.Value(name)
	if b == nil {
		return false
	}
	if valB, ok := b.(bool); ok {
		return valB
	}
	return false
}

func GetBlockNumber(ctx context.Context) *big.Int {
	b := ctx.Value(BlockNumber)
	if b == nil {
		return nil
	}
	if valB, ok := b.(*big.Int); ok {
		return valB
	}
	return nil
}

func GetNoHistoryByBlock(ctx context.Context, currentBlock *big.Int) bool {
	v := ctx.Value(NoHistory)
	if v == nil {
		return true
	}
	if val, ok := v.(noHistFunc); ok {
		return val(currentBlock)
	}
	return true
}

func GetNoHistory(ctx context.Context) bool {
	v := ctx.Value(NoHistory)
	if v == nil {
		return false
	}
	if val, ok := v.(noHistFunc); ok {
		return val(GetBlockNumber(ctx))
	}
	return false
}

type noHistFunc func(currentBlock *big.Int) bool

func getIsNoHistory(defaultValue bool, f noHistFunc) noHistFunc {
	return func(currentBlock *big.Int) bool {
		if defaultValue == true {
			return true
		}

		if f == nil {
			return defaultValue
		}

		return f(currentBlock)
	}
}
