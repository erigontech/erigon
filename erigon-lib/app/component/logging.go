package component

import (
	"path"
	"sync"

	"github.com/rs/zerolog"
	root "github.com/rs/zerolog/log"
)

var logger = path.Base(util.CallerPackageName(0))

func init() {
	app.RegisterLevelUpdater(logger, LogLevel, func() zerolog.Level { return log.GetLevel() })
}

var log = &app.Logger{Logger: logLevel(zerolog.WarnLevel), RWMutex: sync.RWMutex{}}

func logLevel(level zerolog.Level) *zerolog.Logger {
	log := root.With().Str("logger", logger).Logger().Level(level)
	return &log
}

func LogLevel(level zerolog.Level) *zerolog.Logger {
	prev := log.Logger
	log.Lock()
	log.Logger = logLevel(level)
	log.Unlock()
	return prev
}
