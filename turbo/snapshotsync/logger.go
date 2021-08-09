package snapshotsync

import (
	lg "github.com/anacrolix/log"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	lg.Default = NewAdapterLogger()
}
func NewAdapterLogger() lg.Logger {
	return lg.Logger{
		LoggerImpl: lg.LoggerImpl(adapterLogger{}),
	}
}

type adapterLogger struct{}

func (b adapterLogger) Log(msg lg.Msg) {
	lvl, ok := msg.GetLevel()
	if !ok {
		lvl = lg.Info
	}

	switch lvl {
	case lg.Debug:
		log.Info(msg.String())
	case lg.Info:
		log.Info(msg.String())
	case lg.Warning:
		log.Warn(msg.String())
	case lg.Error:
		log.Error(msg.String())
	case lg.Critical:
		log.Error(msg.String())
	default:
		log.Warn("unknown log type", "msg", msg.String())
	}
}
