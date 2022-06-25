package downloadercfg

import (
	"strings"

	utp "github.com/anacrolix/go-libutp"
	lg "github.com/anacrolix/log"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	lg.Default.Handlers = []lg.Handler{adapterHandler{}}
	utp.Logger.Handlers = []lg.Handler{noopHandler{}}
}

func Str2LogLevel(in string) (lg.Level, error) {
	lvl := lg.Level{}
	if err := lvl.UnmarshalText([]byte(in)); err != nil {
		return lvl, err
	}
	return lvl, nil
}

type noopHandler struct{}

func (b noopHandler) Handle(r lg.Record) {
}

type adapterHandler struct{}

func (b adapterHandler) Handle(r lg.Record) {
	lvl := r.Level

	switch lvl {
	case lg.Debug:
		log.Debug(r.String())
	case lg.Info:
		str := r.String()
		if strings.Contains(str, "EOF") ||
			strings.Contains(str, "spurious timer") ||
			strings.Contains(str, "banning ip <nil>") { // suppress useless errors
			break
		}

		log.Info(str)
	case lg.Warning:
		str := r.String()
		if strings.Contains(str, "could not find offer for id") { // suppress useless errors
			break
		}
		if strings.Contains(str, "webrtc conn for unloaded torrent") { // suppress useless errors
			break
		}
		if strings.Contains(str, "TrackerClient closed") { // suppress useless errors
			break
		}
		if strings.Contains(str, "banned ip") { // suppress useless errors
			break
		}
		if strings.Contains(str, "being sole dirtier of piece") { // suppress useless errors
			break
		}

		log.Warn(str)
	case lg.Error:
		str := r.String()
		if strings.Contains(str, "EOF") { // suppress useless errors
			break
		}

		log.Error(str)
	case lg.Critical:
		str := r.String()
		if strings.Contains(str, "EOF") { // suppress useless errors
			break
		}
		if strings.Contains(str, "don't want conns") { // suppress useless errors
			break
		}
		if strings.Contains(str, "torrent closed") { // suppress useless errors
			break
		}

		log.Error(str)
	default:
		log.Debug(r.String(), "torrent_log_type", "unknown")
	}
}
