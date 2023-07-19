package scenarios

import (
	"context"
	"sync"
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
