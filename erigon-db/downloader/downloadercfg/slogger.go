package downloadercfg

import (
	"context"
	"iter"
	"log/slog"
	"slices"

	"github.com/erigontech/erigon-lib/log/v3"
)

func erigonToSlogLevel(from log.Lvl) slog.Level {
	return slog.Level(12 - 4*from)
}

func slogLevelToErigon(from slog.Level) log.Lvl {
	// Go has truncated division. Use bit shift here because it divides toward zero.
	return log.Lvl(3 - (from+3)>>2)
}

type slogHandler struct {
	attrs       []slog.Attr
	enabled     func(level slog.Level, names []string) bool
	modifyLevel func(level *slog.Level, names []string)
}

func (me *slogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	names := me.getNames()
	if me.modifyLevel != nil {
		me.modifyLevel(&level, names)
	}
	return me.enabled(level, names)
}

func (me *slogHandler) Handle(ctx context.Context, record slog.Record) error {
	// Allow tweaking record to fit downstream erigon logging expectations.
	if me.modifyLevel != nil {
		// Should we add the record names here too?
		me.modifyLevel(&record.Level, me.getNames())
	}
	// Unfortunately the *handlers* in Erigon's loggers are loaded with filter levels, which makes
	// it hard to reuse them with our own log level flags. The global root logger here is still
	// instrumented, but it'll do for now. You need to set the console or log file verbosity to get
	// our messages through to the canonical loggers.
	log.Log(slogLevelToErigon(record.Level), record.Message, me.attrsToCtx(record)...)
	return nil
}

func attrToErilogCtxs(keyPrefix string, attr slog.Attr) iter.Seq[any] {
	return func(yield func(any) bool) {
		if attr.Value.Kind() == slog.KindGroup {
			keyPrefix := keyPrefix + attr.Key + "."
			for _, a := range attr.Value.Group() {
				attrToErilogCtxs(keyPrefix, a)(yield)
			}
		} else {
			yield(keyPrefix + attr.Key)
			// This will not unpack nested groups (again). More work required for that.
			yield(attr.Value.Resolve())
		}
	}
}

func (me *slogHandler) attrsToCtx(r slog.Record) (ret []any) {
	// This cap allocation does not take into account group expansion.
	ret = make([]any, 0, 2*(len(me.attrs)+r.NumAttrs()))
	// Add attrs from the logger, then the record, flattening groups because I don't think erilog
	// supports nesting.
	for attr := range chainSeqs(slices.Values(me.attrs), r.Attrs) {
		ret = slices.AppendSeq(ret, attrToErilogCtxs("", attr))
	}
	return
}

func (me *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	ret := *me
	high := len(me.attrs)
	ret.attrs = append(me.attrs[:high:high], attrs...)
	return &ret
}

func (me *slogHandler) WithGroup(name string) slog.Handler {
	// Assuming the no-nesting thing is correct, this would add an implicit key prefix to all new
	// attrs.
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
