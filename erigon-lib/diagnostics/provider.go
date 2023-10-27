package diagnostics

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/log/v3"
)

type ctxKey int

const (
	ckChan ctxKey = iota
)

type Type reflect.Type

type Info interface {
	Type() Type
}

func TypeOf(i Info) Type {
	t := reflect.TypeOf(i)
	return Type(t)
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

	reg, _ := providers[infoType]

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

	reg, _ := providers[infoType]

	toStart := make([]Provider, len(reg.providers))

	for i, provider := range reg.providers {
		toStart[i] = provider
	}

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
		logger.Warn("Diagnostic provider failed", "type", infoType, "err", err)
	}
}

func Send[I Info](ctx context.Context, info I) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	cval := ctx.Value(ckChan)
	if c, ok := cval.(chan I); ok {
		c <- info
	} else {
		return fmt.Errorf("unexpected channel type: %T", cval)
	}

	return nil
}

func Context[I Info](ctx context.Context, buffer int) (context.Context, <-chan I, context.CancelFunc) {
	ch := make(chan I, buffer)
	ctx = context.WithValue(ctx, ckChan, ch)
	ctx, cancel := context.WithCancel(ctx)

	return ctx, ch, func() {
		close(ch)
		cancel()
	}
}
