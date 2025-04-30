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
	return log.Lvl(3 - (from+3)/4)
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
	//me.msgBuf.Reset()
	//err := me.textHandler.Handle(ctx, record)
	//if err != nil {
	//	return err
	//}
	log.Log(slogLevelToErigon(record.Level), record.Message, me.attrsToCtx()...)
	return nil
}

func (me *slogHandler) attrsToCtx() (ret []any) {
	for _, a := range me.attrs {
		ret = append(ret, a.Key, a.Value)
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
