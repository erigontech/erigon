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

var log = app.NewLogger(liblog.LvlWarn, []string{logger}, nil)

func LogLevel(level liblog.Lvl) liblog.Lvl {
	return log.SetLevel(level)
}
