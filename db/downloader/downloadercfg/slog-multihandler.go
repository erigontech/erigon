package downloadercfg

// From https://github.com/golang/go/issues/65954#issuecomment-2786268756.

import (
	"context"
	"errors"
	"log/slog"
)

type multiHandler []slog.Handler

func (h multiHandler) Enabled(ctx context.Context, l slog.Level) bool {
	for i := range h {
		if h[i].Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (h multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for i := range h {
		if h[i].Enabled(ctx, r.Level) {
			if err := h[i].Handle(ctx, r.Clone()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (h multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, 0, len(h))
	for i := range h {
		handlers = append(handlers, h[i].WithAttrs(attrs))
	}
	return multiHandler(handlers)
}

func (h multiHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, 0, len(h))
	for i := range h {
		handlers = append(handlers, h[i].WithGroup(name))
	}
	return multiHandler(handlers)
}
