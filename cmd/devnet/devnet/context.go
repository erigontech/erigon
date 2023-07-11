package devnet

import (
	go_context "context"

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

type cnode struct {
	selector interface{}
	node     Node
}

func WithCurrentNode(ctx go_context.Context, selector interface{}) Context {
	return &context{go_context.WithValue(ctx, ckNode, &cnode{selector: selector})}
}

func WithCliContext(ctx go_context.Context, cliCtx *cli.Context) Context {
	return &context{go_context.WithValue(ctx, ckCliContext, cliCtx)}
}

func CurrentNode(ctx go_context.Context) Node {
	if cn, ok := ctx.Value(ckNode).(*cnode); ok {
		if cn.node == nil {
			if network, ok := ctx.Value(ckNetwork).(*Network); ok {
				cn.node = network.SelectNode(ctx, cn.selector)
			}
		}

		return cn.node
	}

	return nil
}

func SelectNode(ctx go_context.Context, selector ...interface{}) Node {
	if network, ok := ctx.Value(ckNetwork).(*Network); ok {
		if len(selector) > 0 {
			return network.SelectNode(ctx, selector[0])
		}

		if current := CurrentNode(ctx); current != nil {
			return current
		}

		return network.FirstNode()
	}

	return nil
}

func SelectBlockProducer(ctx go_context.Context, selector ...interface{}) Node {
	if network, ok := ctx.Value(ckNetwork).(*Network); ok {
		if len(selector) > 0 {
			blockProducers := network.BlockProducers()
			switch selector := selector[0].(type) {
			case int:
				if selector < len(blockProducers) {
					return blockProducers[selector]
				}
			case NodeSelector:
				for _, node := range blockProducers {
					if selector.Test(ctx, node) {
						return node
					}
				}
			}
		}

		if current := CurrentNode(ctx); current != nil && current.IsBlockProducer() {
			return current
		}

		if blockProducers := network.BlockProducers(); len(blockProducers) > 0 {
			return blockProducers[0]
		}
	}

	return nil
}

func SelectNonBlockProducer(ctx go_context.Context, selector ...interface{}) Node {
	if network, ok := ctx.Value(ckNetwork).(*Network); ok {
		if len(selector) > 0 {
			nonBlockProducers := network.NonBlockProducers()
			switch selector := selector[0].(type) {
			case int:
				if selector < len(nonBlockProducers) {
					return nonBlockProducers[selector]
				}
			case NodeSelector:
				for _, node := range nonBlockProducers {
					if selector.Test(ctx, node) {
						return node
					}
				}
			}
		}

		if current := CurrentNode(ctx); current != nil && !current.IsBlockProducer() {
			return current
		}

		if nonBlockProducers := network.NonBlockProducers(); len(nonBlockProducers) > 0 {
			return nonBlockProducers[0]
		}
	}

	return nil
}
