package params

import (
	"context"
	"math/big"
)

const (
	IsHomesteadEnabled      = "IsHomesteadEnabled"
	IsEIP150Enabled         = "IsEIP150Enabled"
	IsEIP155Enabled         = "IsEIP155Enabled"
	IsEIP158Enabled         = "IsEIP158Enabled"
	IsEIP2027Enabled        = "IsEIP2027Enabled"
	IsByzantiumEnabled      = "IsByzantiumEnabled"
	IsConstantinopleEnabled = "IsConstantinopleEnabled"
	IsPetersburgEnabled     = "IsPetersburgEnabled"
	IsEWASM                 = "IsEWASM"
)

func (c *ChainConfig) WithEIPsEnabledCTX(ctx context.Context, blockNum *big.Int) context.Context {
	ctx = context.WithValue(ctx, IsHomesteadEnabled, c.IsHomestead(blockNum))
	ctx = context.WithValue(ctx, IsEIP150Enabled, c.IsEIP150(blockNum))
	ctx = context.WithValue(ctx, IsEIP155Enabled, c.IsEIP155(blockNum))
	ctx = context.WithValue(ctx, IsEIP158Enabled, c.IsEIP158(blockNum))
	ctx = context.WithValue(ctx, IsEIP2027Enabled, c.IsEIP2027(blockNum))
	ctx = context.WithValue(ctx, IsByzantiumEnabled, c.IsByzantium(blockNum))
	ctx = context.WithValue(ctx, IsConstantinopleEnabled, c.IsConstantinople(blockNum))
	ctx = context.WithValue(ctx, IsPetersburgEnabled, c.IsPetersburg(blockNum))
	ctx = context.WithValue(ctx, IsEWASM, c.IsEWASM(blockNum))
	return ctx
}

func CtxValueToBool(ctx context.Context, name string) bool {
	b := ctx.Value(IsEWASM)
	if b == nil {
		return false
	}
	if valB, ok := b.(bool); ok {
		return valB
	}
	return false
}
