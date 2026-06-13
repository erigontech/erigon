package downloadercfg

import (
	"log/slog"
	"testing"
	"time"

	"github.com/go-quicktest/qt"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestSlogToErigonLevel(t *testing.T) {
	qt.Check(t, qt.Equals(slogLevelToErigon(slog.LevelDebug), log.LvlDebug))
	qt.Check(t, qt.Equals(slogLevelToErigon(slog.LevelInfo), log.LvlInfo))
}

func TestSlogHandlerWithGroup(t *testing.T) {
	h := &slogHandler{
		enabled: func(level slog.Level, names []string) bool { return true },
	}
	grouped, ok := h.WithGroup("torrent").(*slogHandler)
	qt.Assert(t, qt.IsTrue(ok))

	grouped = grouped.WithAttrs([]slog.Attr{
		slog.String("name", "worker"),
		slog.String("piece", "a"),
	}).(*slogHandler)

	var record slog.Record
	record = slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	record.AddAttrs(slog.Int("bytes", 64))

	ctx := grouped.attrsToCtx(record)
	qt.Check(t, qt.DeepEquals(ctxKeys(ctx), []string{"torrent.name", "torrent.piece", "torrent.bytes"}))
	qt.Check(t, qt.DeepEquals(grouped.getNames(), []string{"worker"}))
}

func ctxKeys(ctx []any) (keys []string) {
	for i := 0; i+1 < len(ctx); i += 2 {
		if key, ok := ctx[i].(string); ok {
			keys = append(keys, key)
		}
	}
	return keys
}
