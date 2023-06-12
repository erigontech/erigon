package devnet

import (
	go_context "context"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type ctxKey int

const (
	ckLogger ctxKey = iota
	ckNetwork
	ckNode
	ckCliContext
)

type Context interface {
	go_context.Context
	WithValue(key, value interface{}) Context
}

type context struct {
	go_context.Context
}

func (c *context) WithValue(key, value interface{}) Context {
	return &context{go_context.WithValue(c, key, value)}
}

func AsContext(ctx go_context.Context) Context {
	if ctx, ok := ctx.(Context); ok {
		return ctx
	}

	return &context{ctx}
}

func WithNetwork(ctx go_context.Context, nw *Network) Context {
	return &context{go_context.WithValue(go_context.WithValue(ctx, ckNetwork, nw), ckLogger, nw.Logger)}
}

func Logger(ctx go_context.Context) log.Logger {
	if logger, ok := ctx.Value(ckLogger).(log.Logger); ok {
		return logger
	}

	return log.Root()
}

func WithCurrentNode(ctx go_context.Context, selector ...interface{}) Context {
	return &context{go_context.WithValue(ctx, ckNode, SelectNode(ctx, selector...))}
}

func WithCliContext(ctx go_context.Context, cliCtx *cli.Context) Context {
	return &context{go_context.WithValue(ctx, ckCliContext, cliCtx)}
}

func CurrentNode(ctx go_context.Context) Node {
	if node, ok := ctx.Value(ckNode).(Node); ok {
		return node
	}

	return nil
}

func SelectNode(ctx go_context.Context, selector ...interface{}) Node {
	if network, ok := ctx.Value(ckNetwork).(*Network); ok {
		if len(selector) > 0 {
			switch selector := selector[0].(type) {
			case int:
				return network.SelectNode(ctx, selector)
			case NodeSelector:
				return network.SelectNode(ctx, selector)
			default:
				return nil
			}
		}

		if current := CurrentNode(ctx); current != nil {
			return current
		}

		return network.AnyNode(ctx)
	}

	return nil
}

func SelectMiner(ctx go_context.Context, selector ...interface{}) Node {
	if network, ok := ctx.Value(ckNetwork).(*Network); ok {
		if len(selector) > 0 {
			miners := network.Miners()
			switch selector := selector[0].(type) {
			case int:
				if selector < len(miners) {
					return miners[selector]
				}
			case NodeSelector:
				for _, node := range miners {
					if selector.Test(ctx, node) {
						return node
					}
				}
			}
		}

		if current := CurrentNode(ctx); current != nil && current.IsMiner() {
			return current
		}

		if miners := network.Miners(); len(miners) > 0 {
			return miners[devnetutils.RandomInt(len(miners)-1)]
		}
	}

	return nil
}

func SelectNonMiner(ctx go_context.Context, selector ...interface{}) Node {
	if network, ok := ctx.Value(ckNetwork).(*Network); ok {
		if len(selector) > 0 {
			nonMiners := network.NonMiners()
			switch selector := selector[0].(type) {
			case int:
				if selector < len(nonMiners) {
					return nonMiners[selector]
				}
			case NodeSelector:
				for _, node := range nonMiners {
					if selector.Test(ctx, node) {
						return node
					}
				}
			}
		}

		if current := CurrentNode(ctx); current != nil && !current.IsMiner() {
			return current
		}

		if nonMiners := network.NonMiners(); len(nonMiners) > 0 {
			return nonMiners[devnetutils.RandomInt(len(nonMiners)-1)]
		}
	}

	return nil
}
