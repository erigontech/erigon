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
	HistoryEnabled
	WithHistoryHighest
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

func WithHistory(ctx context.Context, defaultValue bool, f withHistFunc) context.Context {
	return context.WithValue(ctx, HistoryEnabled, getIsWithHistory(defaultValue, f))
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

func GetWithHistoryByBlock(ctx context.Context, currentBlock *big.Int) (context.Context, bool) {
	if currentBlock == nil {
		return ctx, false
	}

	key := getWithHistoryByBlock(currentBlock)
	v, ok := getWithHistoryByKey(ctx, key)
	if ok {
		if v {
			ctx = updateHighestWithHistory(ctx, currentBlock)
		}
		return ctx, v
	}

	return withHistory(ctx, currentBlock, key)
}

func withHistory(ctx context.Context, currentBlock *big.Int, key configKey) (context.Context, bool) {
	v := getWithHistory(ctx, currentBlock)
	ctx = context.WithValue(ctx, key, v)
	if v {
		ctx = updateHighestWithHistory(ctx, currentBlock)
	}

	return ctx, v
}

func updateHighestWithHistory(ctx context.Context, currentBlock *big.Int) context.Context {
	highestWithHistory := getWithHistoryHighest(ctx)
	var currentIsLower bool
	if highestWithHistory != nil {
		currentIsLower = currentBlock.Cmp(highestWithHistory) < 0
	}
	if !currentIsLower {
		ctx = setWithHistoryHighestByBlock(ctx, currentBlock)
	}
	return ctx
}

func getWithHistory(ctx context.Context, currentBlock *big.Int) bool {
	v := ctx.Value(HistoryEnabled)
	if v == nil {
		return false
	}
	if val, ok := v.(withHistFunc); ok {
		return val(currentBlock)
	}
	return false
}

func getWithHistoryHighest(ctx context.Context) *big.Int {
	v := ctx.Value(WithHistoryHighest)
	if v == nil {
		return nil
	}
	if val, ok := v.(*big.Int); ok {
		return val
	}
	return nil
}

func setWithHistoryHighestByBlock(ctx context.Context, block *big.Int) context.Context {
	return context.WithValue(ctx, WithHistoryHighest, block)
}

func getWithHistoryByKey(ctx context.Context, key configKey) (bool, bool) {
	if key < 0 {
		return false, false
	}
	v := ctx.Value(key)
	if v == nil {
		return false, false
	}
	if val, ok := v.(bool); ok {
		return val, true
	}
	return false, false
}

func getWithHistoryByBlock(block *big.Int) configKey {
	if block == nil {
		return configKey(-1)
	}
	return configKey(block.Uint64() + 1000)
}

func GetWithHistory(ctx context.Context) (context.Context, bool) {
	return GetWithHistoryByBlock(ctx, GetBlockNumber(ctx))
}

type withHistFunc func(currentBlock *big.Int) bool

func getIsWithHistory(defaultValue bool, f withHistFunc) withHistFunc {
	return func(currentBlock *big.Int) bool {
		if f == nil {
			return defaultValue
		}

		return f(currentBlock)
	}
}
