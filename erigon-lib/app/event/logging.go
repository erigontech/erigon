package event

import (
	"github.com/rs/zerolog"

	"github.com/erigontech/erigon-lib/app"
	root "github.com/rs/zerolog/log"
)

const (
	Logger = "reactor/events"
)

func init() {
	app.RegisterLevelUpdater(Logger, LogLevel, func() zerolog.Level { return log.GetLevel() })
}

var log = logLevel(zerolog.WarnLevel)

func logLevel(level zerolog.Level) *zerolog.Logger {
	log := root.With().Str("package", "reactor/events").Logger().Level(level)
	return &log
}

func LogLevel(level zerolog.Level) *zerolog.Logger {
	prev := log
	log = logLevel(level)
	return prev
}
