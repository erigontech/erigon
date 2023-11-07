package diagnostics

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/log/v3"
)

type ctxKey int

const (
	ckChan ctxKey = iota
)

type Type interface {
	reflect.Type
	Context() context.Context
	Err() error
}

type diagType struct {
	reflect.Type
}

var cancelled = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

func (t diagType) Context() context.Context {
	providerMutex.Lock()
	defer providerMutex.Unlock()
	if reg := providers[t]; reg != nil {
		return reg.context
	}

	return cancelled
}

func (t diagType) Err() error {
	return t.Context().Err()
}

type Info interface {
	Type() Type
}

func TypeOf(i Info) Type {
	t := reflect.TypeOf(i)
	return diagType{t}
}

type Provider interface {
	StartDiagnostics(ctx context.Context) error
}

type ProviderFunc func(ctx context.Context) error

func (f ProviderFunc) StartDiagnostics(ctx context.Context) error {
	return f(ctx)
}

type registry struct {
	context   context.Context
	providers []Provider
}

var providers = map[Type]*registry{}
var providerMutex sync.RWMutex

func RegisterProvider(provider Provider, infoType Type, logger log.Logger) {
	providerMutex.Lock()
	defer providerMutex.Unlock()

	reg := providers[infoType]

	if reg != nil {
		for _, p := range reg.providers {
			if p == provider {
				return
			}
		}
	} else {
		reg = &registry{}
		providers[infoType] = reg
	}

	reg.providers = append(reg.providers, provider)

	if reg.context != nil {
		go startProvider(reg.context, infoType, provider, logger)
	}
}

func StartProviders(ctx context.Context, infoType Type, logger log.Logger) {
	providerMutex.Lock()

	reg := providers[infoType]
	if reg == nil {
		reg = &registry{}
		providers[infoType] = reg
	}

	toStart := make([]Provider, len(reg.providers))
	copy(toStart, reg.providers)

	reg.context = ctx

	providerMutex.Unlock()

	for _, provider := range toStart {
		go startProvider(ctx, infoType, provider, logger)
	}
}

func startProvider(ctx context.Context, infoType Type, provider Provider, logger log.Logger) {
	defer func() {
		if rec := recover(); rec != nil {
			err := fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
			logger.Warn("Diagnostic provider failed", "type", infoType, "err", err)
		}
	}()

	if err := provider.StartDiagnostics(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			logger.Warn("Diagnostic provider failed", "type", infoType, "err", err)
		}
	}
}

func Send[I Info](info I) error {
	ctx := info.Type().Context()

	if ctx.Err() != nil {
		if !errors.Is(ctx.Err(), context.Canceled) {
			// drop the diagnostic message if there is
			// no active diagnostic context for the type
			return nil
		}

		return ctx.Err()
	}

	cval := ctx.Value(ckChan)

	if cp, ok := cval.(*atomic.Pointer[chan I]); ok {
		if c := (*cp).Load(); c != nil {
			select {
			case *c <- info:
			default:
				// drop the diagnostic message if the receiver is busy
				// so the sender is not blocked on non critcal actions
			}
		}
	} else {
		return fmt.Errorf("unexpected channel type: %T", cval)
	}

	return nil
}

func Context[I Info](ctx context.Context, buffer int) (context.Context, <-chan I, context.CancelFunc) {
	c := make(chan I, buffer)
	cp := atomic.Pointer[chan I]{}
	cp.Store(&c)

	ctx = context.WithValue(ctx, ckChan, &cp)
	ctx, cancel := context.WithCancel(ctx)

	return ctx, *cp.Load(), func() {
		cancel()

		if cp.CompareAndSwap(&c, nil) {
			ch := c
			c = nil
			close(ch)
		}
	}
}
