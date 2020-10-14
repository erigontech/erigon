package torrent

import (
	lg "github.com/anacrolix/log"
	"github.com/ledgerwatch/turbo-geth/log"
)

func init() {
	lg.Default = NewLogger()
}
func NewLogger() lg.Logger {
	return lg.Logger{
		lg.LoggerImpl(btLogger{}),
	}
}

type btLogger struct{}

func (b btLogger) Log(msg lg.Msg) {
	lvl, ok := msg.GetLevel()
	if !ok {
		lvl = lg.Debug
	}

	switch lvl {
	case lg.Debug:
		log.Debug(msg.String())
	case lg.Info:
		log.Info(msg.String())
	case lg.Warning:
		log.Warn(msg.String())
	case lg.Error:
		log.Error(msg.String())
	case lg.Critical:
		log.Error(msg.String())
	default:
		log.Warn("unknown log type")
		log.Warn(msg.String())
	}
}
