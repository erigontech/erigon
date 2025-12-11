// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package diaglib

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
)

type ctxKey int

const (
	ckChan ctxKey = iota
)

type Type interface {
	reflect.Type
	Context() context.Context
	Err() error
	Enabled() bool
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

func (t diagType) Enabled() bool {
	return t.Err() == nil
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

func Send[I Info](info I) {
	defer func() {
		if r := recover(); r != nil {
			log.Debug("diagnostic Send panic recovered: %v, stack: %s", r, dbg.Stack())
		}
	}()

	ctx := info.Type().Context()

	if ctx.Err() != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return
		}

		log.Debug("diagnostic send failed: context error", "err", ctx.Err())
	}

	cval := ctx.Value(ckChan)

	if c, ok := cval.(chan I); ok {
		select {
		case c <- info:
		default:
			// drop the diagnostic message if the receiver is busy
			// so the sender is not blocked on non critcal actions
		}
	} else {
		if cval == nil {
			return
		}

		log.Debug(fmt.Sprintf("unexpected channel type: %T", cval))
	}
}

func Context[I Info](ctx context.Context, buffer int) (context.Context, <-chan I, context.CancelFunc) {
	c := make(chan I, buffer)
	l := sync.Mutex{}

	ctx = context.WithValue(ctx, ckChan, c)
	ctx, cancel := context.WithCancel(ctx)

	return ctx, c, func() {
		cancel()

		l.Lock()
		defer l.Unlock()
		if c != nil {
			ch := c
			c = nil
			close(ch)
		}
	}
}
