package params

import (
	"context"
	"math/big"
)

type configKey int

const (
	IsHomesteadEnabled configKey = iota
	IsTangerineWhistleEnabled
	IsSpuriousDragonEnabled
	IsByzantiumEnabled
	IsConstantinopleEnabled
	IsPetersburgEnabled
	BlockNumber
	WithHistoryHighest
)

func (c *ChainConfig) WithEIPsFlags(ctx context.Context, blockNum uint64) context.Context {
	ctx = context.WithValue(ctx, IsHomesteadEnabled, c.IsHomestead(blockNum))
	ctx = context.WithValue(ctx, IsTangerineWhistleEnabled, c.IsTangerineWhistle(blockNum))
	ctx = context.WithValue(ctx, IsSpuriousDragonEnabled, c.IsSpuriousDragon(blockNum))
	ctx = context.WithValue(ctx, IsByzantiumEnabled, c.IsByzantium(blockNum))
	ctx = context.WithValue(ctx, IsConstantinopleEnabled, c.IsConstantinople(blockNum))
	ctx = context.WithValue(ctx, IsPetersburgEnabled, c.IsPetersburg(blockNum))
	ctx = context.WithValue(ctx, BlockNumber, blockNum)
	return ctx
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
