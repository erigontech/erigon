package component

import (
	"path"

	"github.com/erigontech/erigon-lib/app"
	"github.com/erigontech/erigon-lib/app/util"
	liblog "github.com/erigontech/erigon-lib/log/v3"
)

var logger = path.Base(util.CallerPackageName(0))

func init() {
	app.RegisterLevelUpdater(logger, LogLevel, func() liblog.Lvl { return log.GetLevel() })
}

var log = app.NewLogger(logLevel(liblog.LvlWarn))

func logLevel(level liblog.Lvl) (liblog.Logger, liblog.Lvl) {
	return liblog.New(liblog.Root()), level 
	//TODO .With().Str("logger", logger).Logger().Level(level)
}

func LogLevel(level liblog.Lvl) liblog.Lvl {
	return log.SetLevel(level)
}
