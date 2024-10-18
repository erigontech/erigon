// Copyright 2021 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package downloadercfg

import (
	"fmt"
	"strings"

	lg "github.com/anacrolix/log"

	"github.com/erigontech/erigon-lib/log/v3"
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
			log.Trace(str, "lvl", lvl.LogString())
			break
		}
		log.Debug(str)
	case lg.Info:
		str := r.String()
		skip := strings.Contains(str, "EOF")
		//strings.Contains(str, "banning ip <nil>") ||
		//strings.Contains(str, "spurious timer") { // suppress useless errors
		if skip {
			log.Trace(str, "lvl", lvl.LogString())
			break
		}
		log.Info(str)
	case lg.Warning:
		str := r.String()
		skip := strings.Contains(str, "EOF") ||
			strings.Contains(str, "requested chunk too long") ||
			strings.Contains(str, "banned ip") ||
			//strings.Contains(str, "banning webseed") ||
			strings.Contains(str, "TrackerClient closed") ||
			strings.Contains(str, "being sole dirtier of piece") ||
			strings.Contains(str, "webrtc conn for unloaded torrent") ||
			strings.Contains(str, "could not find offer for id") ||
			strings.Contains(str, "received invalid reject") ||
			strings.Contains(str, "reservation cancelled")

		if skip {
			log.Debug(str)
			break
		}
		log.Warn(str)
	case lg.Error:
		str := r.String()
		skip := strings.Contains(str, "EOF") ||
			strings.Contains(str, "short write") ||
			strings.Contains(str, "disabling data download")
		if skip {
			log.Trace(str, "lvl", lvl.LogString())
			break
		}
		log.Error(str)
	case lg.Critical:
		str := r.String()
		skip := strings.Contains(str, "EOF") ||
			strings.Contains(str, "torrent closed") ||
			strings.Contains(str, "don't want conns")
		if skip {
			log.Trace(str, "lvl", lvl.LogString())
			break
		}
		log.Error(str)
	default:
		str := r.String()
		skip := strings.Contains(str, "EOF") || strings.Contains(str, "unhandled response status") ||
			strings.Contains(str, "error doing webseed request")
		if skip {
			log.Trace(str, "lvl", lvl.LogString())
			break
		}
		log.Info("[downloader] "+r.String(), "torrent_log_type", "unknown", "or", lvl.LogString())
	}
}

type RetryableHttpLogger struct {
	l log.Logger
}

func NewRetryableHttpLogger(l log.Logger) *RetryableHttpLogger {
	return &RetryableHttpLogger{l: l}
}

func (l *RetryableHttpLogger) Error(msg string, keysAndValues ...interface{}) {
	l.l.Debug(msg, keysAndValues...)
}
func (l *RetryableHttpLogger) Warn(msg string, keysAndValues ...interface{}) {
	l.l.Debug(msg, keysAndValues...)
}
func (l *RetryableHttpLogger) Info(msg string, keysAndValues ...interface{}) {
	l.l.Debug(msg, keysAndValues...)
}
func (l *RetryableHttpLogger) Debug(msg string, keysAndValues ...interface{}) {
	l.l.Trace(msg, keysAndValues...)
}
