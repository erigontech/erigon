package downloadercfg

import (
	"log/slog"
	"testing"

	"github.com/go-quicktest/qt"

	"github.com/erigontech/erigon-lib/log/v3"
)

func TestSlogToErigonLevel(t *testing.T) {
	qt.Check(t, qt.Equals(slogLevelToErigon(slog.LevelDebug), log.LvlDebug))
	qt.Check(t, qt.Equals(slogLevelToErigon(slog.LevelInfo), log.LvlInfo))
}
