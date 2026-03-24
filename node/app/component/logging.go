package component

import (
	"path"

	liblog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/app"
	"github.com/erigontech/erigon/node/app/util"
)

var logger = path.Base(util.CallerPackageName(0))

var log = app.NewLogger(liblog.LvlWarn, []string{logger}, nil)

func LogLevel(level liblog.Lvl) liblog.Lvl {
	return log.SetLevel(level)
}
