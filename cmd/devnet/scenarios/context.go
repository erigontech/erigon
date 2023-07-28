package scenarios

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
)

type ctxKey int

const (
	ckParams ctxKey = iota
)

func stepRunners(ctx context.Context) []*stepRunner {
	return nil
}

func ContextValues(ctx context.Context) []context.Context {
	if ctx == nil {
		return nil
	}

	if compound, ok := ctx.(*CompoundContext); ok {
		var contexts []context.Context

		for context := range compound.contexts {
			contexts = append(contexts, context)
		}

		return contexts
	}

	return []context.Context{ctx}
}

var empty struct{}

type CompoundContext struct {
	context.Context
	contexts map[context.Context]struct{}
	mutex    sync.RWMutex
}

func (join *CompoundContext) Err() error {
	if join.Context.Err() != nil {
		return join.Context.Err()
	}

	join.mutex.RLock()
	defer join.mutex.RUnlock()
	for context := range join.contexts {
		if context.Err() != nil {
			return context.Err()
		}
	}

	return nil
}

func (join *CompoundContext) Value(key interface{}) interface{} {
	join.mutex.RLock()
	defer join.mutex.RUnlock()
	for context := range join.contexts {
		if value := context.Value(key); value != nil {
			return value
		}
	}

	return join.Context.Value(key)
}

var background = context.Background()

func JoinContexts(ctx context.Context, others ...context.Context) context.Context {
	var join *CompoundContext

	if ctx != nil {
		if compound, ok := ctx.(*CompoundContext); ok {
			join = &CompoundContext{compound.Context, map[context.Context]struct{}{}, sync.RWMutex{}}
			compound.mutex.RLock()
			for context := range compound.contexts {
				join.contexts[context] = empty
			}
			compound.mutex.RUnlock()
		} else {
			join = &CompoundContext{background, map[context.Context]struct{}{ctx: empty}, sync.RWMutex{}}
		}
	} else {
		join = &CompoundContext{background, map[context.Context]struct{}{}, sync.RWMutex{}}
	}

	for _, context := range others {
		if compound, ok := context.(*CompoundContext); ok {
			if compound.Context != background {
				join.contexts[compound.Context] = empty
			}

			compound.mutex.RLock()
			for context := range compound.contexts {
				if context != background {
					join.contexts[context] = empty
				}
			}
			compound.mutex.RUnlock()
		} else if context != nil && context != background {
			join.contexts[context] = empty
		}
	}

	return join
}

type Context interface {
	devnet.Context
	WithParam(name string, value interface{}) Context
}

type scenarioContext struct {
	devnet.Context
}

func (c scenarioContext) WithParam(name string, value interface{}) Context {
	return WithParam(c, name, value)
}

type Params map[string]interface{}

func WithParam(ctx context.Context, name string, value interface{}) Context {
	if params, ok := ctx.Value(ckParams).(Params); ok {
		params[name] = value
		if ctx, ok := ctx.(scenarioContext); ok {
			return ctx
		}

		return scenarioContext{devnet.AsContext(ctx)}
	}

	ctx = context.WithValue(ctx, ckParams, Params{name: value})
	return scenarioContext{devnet.AsContext(ctx)}
}

func Param[P any](ctx context.Context, name string) (P, bool) {
	if params, ok := ctx.Value(ckParams).(Params); ok {
		if param, ok := params[name]; ok {
			return param.(P), true
		}
	}

	var p P
	return p, false
}
