package devnet

import (
	context "context"

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

func WithNetwork(ctx context.Context, nw *Network) context.Context {
	return context.WithValue(context.WithValue(ctx, ckNetwork, nw), ckLogger, nw.Logger)
}

func Logger(ctx context.Context) log.Logger {
	if logger, ok := ctx.Value(ckLogger).(log.Logger); ok {
		return logger
	}

	return log.Root()
}

type cnode struct {
	selector interface{}
	node     Node
}

func WithCurrentNode(ctx context.Context, selector interface{}) context.Context {
	return context.WithValue(ctx, ckNode, &cnode{selector: selector})
}

func WithCliContext(ctx context.Context, cliCtx *cli.Context) context.Context {
	return context.WithValue(ctx, ckCliContext, cliCtx)
}

func CliContext(ctx context.Context) *cli.Context {
	return ctx.Value(ckCliContext).(*cli.Context)
}

func CurrentNode(ctx context.Context) Node {
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

func SelectNode(ctx context.Context, selector ...interface{}) Node {
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

func SelectBlockProducer(ctx context.Context, selector ...interface{}) Node {
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

func SelectNonBlockProducer(ctx context.Context, selector ...interface{}) Node {
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
