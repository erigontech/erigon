package exec

import "context"

type blockGasKey struct{}

// BlockGas is what the in-progress block has LEFT of its gas, handed to a round that is going to add
// to that block.
//
// ⚠ IT TRAVELS WITH THE ROUND, NOT WITH THE STATE. A block is built over many rounds and each round
// is its own execution run, so a gas pool built per run starts every round at the full block limit
// and the block's own budget is never tracked — a body can be built well past its limit with nothing
// in execution saying so. The budget belongs to the BLOCK, and the only thing scoped to one block
// here is the round itself: SharedDomains spans a whole batch of them on the canonical path, so
// block-scoped state put there is only accidentally correct.
//
// Nothing is handed back. What the block has spent is already recorded, transaction by transaction,
// in the receipts it accumulated — so the next round's remaining is derived, not carried.
type BlockGas struct {
	Regular uint64
	State   uint64
	Blob    uint64
	// Full is called when the block has no room for the round's next transaction. Gas-full, which is
	// not out-of-gas: that transaction is fine and the block is what ran out.
	Full func()
}

// WithBlockGas gives a round the block's remaining budget.
func WithBlockGas(ctx context.Context, g BlockGas) context.Context {
	return context.WithValue(ctx, blockGasKey{}, g)
}

// BlockGasFrom reports the block's remaining budget, and whether the caller tracks one at all. Not
// tracked means this is not a round of a multi-round block, and the block's own limit stands.
func BlockGasFrom(ctx context.Context) (BlockGas, bool) {
	g, ok := ctx.Value(blockGasKey{}).(BlockGas)
	return g, ok
}
