package downloadercfg

import (
	"fmt"
	"strings"

	utp "github.com/anacrolix/go-libutp"
	lg "github.com/anacrolix/log"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	lg.Default.Handlers = []lg.Handler{adapterHandler{}}
	utp.Logger.Handlers = []lg.Handler{noopHandler{}}
}

func Int2LogLevel(level int) (lvl lg.Level, dbg bool, err error) {
	switch level {
	case 0:
		lvl = lg.Critical
	case 1:
		lvl = lg.Error
	case 2:
		lvl = lg.Warning
	case 3:
		lvl = lg.Info
	case 4:
		lvl = lg.Debug
	case 5:
		lvl = lg.Debug
		dbg = true
	default:
		return lvl, dbg, fmt.Errorf("invalid level set, expected a number between 0-5 but got: %d", level)
	}
	return lvl, dbg, nil
}

type noopHandler struct{}

func (b noopHandler) Handle(r lg.Record) {
}

type adapterHandler struct{}

func (b adapterHandler) Handle(r lg.Record) {
	lvl := r.Level

	switch lvl {
	case lg.Debug:
		log.Info("[downloader] " + r.String())
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
		if strings.Contains(str, "requested chunk too long") { // suppress useless errors
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
		log.Info("[downloader] "+r.String(), "torrent_log_type", "unknown", "or", lvl.LogString())
	}
}
