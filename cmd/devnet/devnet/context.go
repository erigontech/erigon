package devnet

import (
	"context"
	"math/big"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type ctxKey int

const (
	ckLogger ctxKey = iota
	ckNetwork
	ckNode
	ckCliContext
	ckDevnet
)

type Context interface {
	context.Context
	WithValue(key, value interface{}) Context
	WithCurrentNetwork(selector interface{}) Context
	WithCurrentNode(selector interface{}) Context
}

type devnetContext struct {
	context.Context
}

func (c devnetContext) WithValue(key, value interface{}) Context {
	return devnetContext{context.WithValue(c, key, value)}
}

func (c devnetContext) WithCurrentNetwork(selector interface{}) Context {
	return WithCurrentNetwork(c, selector)
}

func (c devnetContext) WithCurrentNode(selector interface{}) Context {
	return WithCurrentNode(c, selector)
}

func WithNetwork(ctx context.Context, nw *Network) Context {
	return devnetContext{context.WithValue(context.WithValue(ctx, ckNetwork, nw), ckLogger, nw.Logger)}
}

func AsContext(ctx context.Context) Context {
	if ctx, ok := ctx.(Context); ok {
		return ctx
	}

	return devnetContext{ctx}
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

type cnet struct {
	selector interface{}
	network  *Network
}

func WithDevnet(ctx context.Context, cliCtx *cli.Context, devnet Devnet, logger log.Logger) Context {
	return WithCliContext(
		context.WithValue(
			context.WithValue(ctx, ckDevnet, devnet),
			ckLogger, logger), cliCtx)
}

func WithCurrentNetwork(ctx context.Context, selector interface{}) Context {
	if current := CurrentNetwork(ctx); current != nil {
		if devnet, ok := ctx.Value(ckDevnet).(Devnet); ok {
			selected := devnet.SelectNetwork(ctx, selector)

			if selected == current {
				if ctx, ok := ctx.(devnetContext); ok {
					return ctx
				}
				return devnetContext{ctx}
			}
		}
	}

	if current := CurrentNode(ctx); current != nil {
		ctx = context.WithValue(ctx, ckNode, nil)
	}

	return devnetContext{context.WithValue(ctx, ckNetwork, &cnet{selector: selector})}
}

func WithCurrentNode(ctx context.Context, selector interface{}) Context {
	if node, ok := selector.(Node); ok {
		return devnetContext{context.WithValue(ctx, ckNode, &cnode{node: node})}
	}

	return devnetContext{context.WithValue(ctx, ckNode, &cnode{selector: selector})}
}

func WithCliContext(ctx context.Context, cliCtx *cli.Context) Context {
	return devnetContext{context.WithValue(ctx, ckCliContext, cliCtx)}
}

func CliContext(ctx context.Context) *cli.Context {
	return ctx.Value(ckCliContext).(*cli.Context)
}

func CurrentChainID(ctx context.Context) *big.Int {
	if network := CurrentNetwork(ctx); network != nil {
		return network.ChainID()
	}

	return &big.Int{}
}

func CurrentChainName(ctx context.Context) string {
	if network := CurrentNetwork(ctx); network != nil {
		return network.Chain
	}

	return ""
}

func Networks(ctx context.Context) []*Network {
	if devnet, ok := ctx.Value(ckDevnet).(Devnet); ok {
		return devnet
	}

	return nil
}

func CurrentNetwork(ctx context.Context) *Network {
	if cn, ok := ctx.Value(ckNetwork).(*cnet); ok {
		if cn.network == nil {
			if devnet, ok := ctx.Value(ckDevnet).(Devnet); ok {
				cn.network = devnet.SelectNetwork(ctx, cn.selector)
			}
		}

		return cn.network
	}

	if current := CurrentNode(ctx); current != nil {
		if n, ok := current.(*node); ok {
			return n.network
		}
	}

	return nil
}

func CurrentNode(ctx context.Context) Node {
	if cn, ok := ctx.Value(ckNode).(*cnode); ok {
		if cn.node == nil {
			if network := CurrentNetwork(ctx); network != nil {
				cn.node = network.SelectNode(ctx, cn.selector)
			}
		}

		return cn.node
	}

	return nil
}

func SelectNode(ctx context.Context, selector ...interface{}) Node {
	if network := CurrentNetwork(ctx); network != nil {
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
	if network := CurrentNetwork(ctx); network != nil {
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
	if network := CurrentNetwork(ctx); network != nil {
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
