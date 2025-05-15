package downloadercfg

import (
	"context"
	"github.com/erigontech/erigon-lib/log/v3"
	"log/slog"
)

func erigonToSlogLevel(from log.Lvl) slog.Level {
	return slog.Level(12 - 4*from)
}

func slogLevelToErigon(from slog.Level) log.Lvl {
	// Fuck sake Go has truncated division. Use bit shift here because it divides toward zero and we
	// can.
	return log.Lvl(3 - (from+3)>>2)
}

type slogHandler struct {
	//minLevel slog.Level
	//msgBuf      bytes.Buffer
	//textHandler *slog.TextHandler
	attrs   []slog.Attr
	enabled func(level slog.Level, names []string) bool
}

func (me *slogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return me.enabled(level, me.getNames())
}

func (me *slogHandler) Handle(ctx context.Context, record slog.Record) error {
	// Unfortunately the *handlers* in Erigon's loggers are loaded with filter levels, which makes
	// it hard to reuse them with our own log level flags. The global root logger here is still
	// instrumented but it'll do for now. You need to set the console or log file verbosity to get
	// our messages through to the canonical loggers.
	log.Log(slogLevelToErigon(record.Level), record.Message, me.attrsToCtx(record)...)
	return nil
}

func (me *slogHandler) attrsToCtx(r slog.Record) (ret []any) {
	ret = make([]any, 0, 2*(len(me.attrs)+r.NumAttrs()))
	for _, a := range me.attrs {
		ret = append(ret, a.Key, a.Value)
	}
	r.Attrs(func(attr slog.Attr) bool {
		ret = append(ret, attr.Key, attr.Value)
		return true
	})
	return
}

func (me *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	ret := *me
	high := len(me.attrs)
	ret.attrs = append(me.attrs[:high:high], attrs...)
	return &ret
}

func (me *slogHandler) WithGroup(name string) slog.Handler {
	// Let's see if we should use a TextHandler and write it by hand for Erigon first...
	panic("implement me")
}

func (me *slogHandler) getNames() (names []string) {
	for _, a := range me.attrs {
		if a.Key == "name" {
			names = append(names, a.Value.String())
		}
	}
	return
}
