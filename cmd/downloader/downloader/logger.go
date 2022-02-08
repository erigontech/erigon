package downloader

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

var String2LogLevel = map[string]lg.Level{
	lg.Debug.LogString():   lg.Debug,
	lg.Info.LogString():    lg.Info,
	lg.Warning.LogString(): lg.Warning,
	lg.Error.LogString():   lg.Error,
}

type adapterLogger struct{}

func (b adapterLogger) Log(msg lg.Msg) {
	lvl, ok := msg.GetLevel()
	if !ok {
		lvl = lg.Debug
	}

	switch lvl {
	case lg.Debug:
		log.Debug(msg.String())
	case lg.Info:
		str := msg.String()
		if strings.Contains(str, "EOF") ||
			strings.Contains(str, "spurious timer") ||
			strings.Contains(str, "banning ip <nil>") { // suppress useless errors
			break
		}

		log.Info(str)
	case lg.Warning:
		str := msg.String()
		if strings.Contains(str, "could not find offer for id") { // suppress useless errors
			break
		}

		log.Warn(str)
	case lg.Error:
		str := msg.String()
		if strings.Contains(str, "EOF") { // suppress useless errors
			break
		}

		log.Error(str)
	case lg.Critical:
		str := msg.String()
		if strings.Contains(str, "EOF") { // suppress useless errors
			break
		}
		if strings.Contains(str, "don't want conns") { // suppress useless errors
			break
		}

		log.Error(str)
	default:
		log.Warn("unknown log type", "msg", msg.String())
	}
}

// NullWriter implements the io.Write interface but doesn't do anything.
type NullWriter int

// Write implements the io.Write interface but is a noop.
func (NullWriter) Write([]byte) (int, error) { return 0, nil }
