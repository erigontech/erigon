/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package downloadercfg

import (
	"fmt"
	"strings"

	lg "github.com/anacrolix/log"
	"github.com/ledgerwatch/log/v3"
)

func init() {
	lg.Default.Handlers = []lg.Handler{adapterHandler{}}
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
		str := r.String()
		skip := strings.Contains(str, "completion change") || strings.Contains(str, "hashed piece") ||
			strings.Contains(str, "set torrent=") ||
			strings.Contains(str, "all initial dials failed") ||
			strings.Contains(str, "local and remote peer ids are the same") ||
			strings.Contains(str, "connection at") || strings.Contains(str, "don't want conns right now") ||
			strings.Contains(str, "is mutually complete") ||
			strings.Contains(str, "sending PEX message") || strings.Contains(str, "received pex message") ||
			strings.Contains(str, "announce to") || strings.Contains(str, "announcing to") ||
			strings.Contains(str, "EOF") || strings.Contains(str, "closed") || strings.Contains(str, "connection reset by peer") || strings.Contains(str, "use of closed network connection") || strings.Contains(str, "broken pipe") ||
			strings.Contains(str, "inited with remoteAddr")
		if skip {
			log.Trace(str)
			break
		}
		log.Debug(str)
	case lg.Info:
		str := r.String()
		skip := strings.Contains(str, "EOF")
		//strings.Contains(str, "banning ip <nil>") ||
		//strings.Contains(str, "spurious timer") { // suppress useless errors
		if skip {
			log.Trace(str)
			break
		}
		log.Info(str)
	case lg.Warning:
		str := r.String()
		skip := strings.Contains(str, "EOF") ||
			strings.Contains(str, "requested chunk too long")

		//if strings.Contains(str, "could not find offer for id") { // suppress useless errors
		//	break
		//}
		//if strings.Contains(str, "webrtc conn for unloaded torrent") { // suppress useless errors
		//	break
		//}
		//if strings.Contains(str, "TrackerClient closed") { // suppress useless errors
		//	break
		//}
		//if strings.Contains(str, "banned ip") { // suppress useless errors
		//	break
		//}
		//if strings.Contains(str, "being sole dirtier of piece") { // suppress useless errors
		//	break
		//}
		//if strings.Contains(str, "reservation cancelled") { // suppress useless errors
		//	break
		//}
		//if strings.Contains(str, "received invalid reject") { // suppress useless errors
		//	break
		//}

		if skip {
			log.Trace(str)
			break
		}
		log.Warn(str)
	case lg.Error:
		str := r.String()
		skip := strings.Contains(str, "EOF")
		if skip {
			log.Trace(str)
			break
		}
		log.Error(str)
	case lg.Critical:
		str := r.String()
		skip := strings.Contains(str, "EOF") ||
			strings.Contains(str, "torrent closed") ||
			strings.Contains(str, "don't want conns")
		if skip {
			log.Trace(str)
			break
		}
		log.Error(str)
	default:
		str := r.String()
		skip := strings.Contains(str, "EOF") || strings.Contains(str, "unhandled response status")
		if skip {
			log.Trace(str)
			break
		}
		log.Info("[downloader] "+r.String(), "torrent_log_type", "unknown", "or", lvl.LogString())
	}
}
