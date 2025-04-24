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

func anacrolixToErigonLogLevel(level lg.Level) log.Lvl {
	switch level {
	case lg.Never:
		// This should never occur. Maybe log that a message leaked?
		panic(level)
		return log.LvlTrace + 1
	case lg.NotSet:
		// This is usually bad practice. Set an appropriate default so it doesn't happen.
		return log.LvlWarn
	case lg.Debug:
		return log.LvlDebug
	case lg.Info:
		return log.LvlInfo
	case lg.Warning:
		return log.LvlWarn
	case lg.Error:
		return log.LvlError
	case lg.Critical:
		return log.LvlCrit
	case lg.Disabled:
		panic(level)
		// This should never occur. Maybe log that a message leaked?
		return log.LvlError
	default:
		// This shouldn't happen...
		panic(level)
	}
}

type noopHandler struct{}

func (b noopHandler) Handle(r lg.Record) {
}

type adapterHandler struct{}

func (b adapterHandler) Handle(r lg.Record) {
	// Note that anacrolix/log now partly supports stdlib slog, so if Erigon switches to that, it
	// would be better to use that too. anacrolix/torrent might change eventually too.
	lvl := anacrolixToErigonLogLevel(r.Level)
	// TODO: anacrolix/log has context values we can pull out... Also can we use the two line
	// formatter or a newer implementation for the single line?
	msg := "[downloader torrent client] " + string(lg.LineFormatter(r))
	log.Log(lvl, msg)
}

// TODO: Ditch this.
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
