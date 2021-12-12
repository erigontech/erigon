package snapshotsync

import (
	stdlog "log"
	"strings"

	utp "github.com/anacrolix/go-libutp"
	lg "github.com/anacrolix/log"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	lg.Default = NewAdapterLogger()
	utp.Logger = stdlog.New(NullWriter(1), "", stdlog.LstdFlags)
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
		if strings.Contains(msg.String(), "could not find offer for id") { // suppress useless errors
			break
		}

		log.Warn(msg.String())
	case lg.Error:
		if strings.Contains(msg.String(), "EOF") { // suppress useless errors
			break
		}

		log.Error(msg.String())
	case lg.Critical:
		if strings.Contains(msg.String(), "EOF") { // suppress useless errors
			break
		}
		if strings.Contains(msg.String(), "don't want conns") { // suppress useless errors
			break
		}

		log.Error(msg.String())
	default:
		log.Warn("unknown log type", "msg", msg.String())
	}
}

// NullWriter implements the io.Write interface but doesn't do anything.
type NullWriter int

// Write implements the io.Write interface but is a noop.
func (NullWriter) Write([]byte) (int, error) { return 0, nil }
