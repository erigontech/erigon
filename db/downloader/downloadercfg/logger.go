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
	"bytes"
	"fmt"

	analog "github.com/anacrolix/log"

	"github.com/erigontech/erigon-lib/log/v3"
)

func init() {
	// Erigon's inherited log library does filtering per handler. anacrolix/log does filtering at
	// the logger level...
	analog.Default.Handlers = []analog.Handler{adapterHandler{}}
}

// Converts flag int to Erigon log level. Surely there's a helper for this somewhere else...
func Int2LogLevel(level int) (lvl log.Lvl, err error) {
	if level < 0 || level > 5 {
		err = fmt.Errorf("invalid level set, expected a number between 0-5 but got: %d", level)
		return
	}
	lvl = log.Lvl(level)
	return
}

// dbg was an old field I guess used for the fact analog doesn't have a trace level. Probably don't
// need it anymore.
func erigonToAnalogLevel(level log.Lvl) (lvl analog.Level, dbg bool, err error) {
	switch level {
	case log.LvlCrit:
		lvl = analog.Critical
	case log.LvlError:
		lvl = analog.Error
	case log.LvlWarn:
		lvl = analog.Warning
	case log.LvlInfo:
		lvl = analog.Info
	case log.LvlDebug:
		lvl = analog.Debug
	case log.LvlTrace:
		// Should this be analog.NotSet?
		lvl = analog.Debug
		dbg = true
	default:
		return lvl, dbg, fmt.Errorf("invalid level set, expected a number between 0-5 but got: %d", level)
	}
	return lvl, dbg, nil
}

func anacrolixToErigonLogLevel(level analog.Level) log.Lvl {
	switch level {
	case analog.Never:
		// This should never occur. Maybe log that a message leaked?
		panic(level)
		//nolint
		return log.LvlTrace + 1
	case analog.NotSet:
		// This is usually bad practice. Set an appropriate default so it doesn't happen.
		return log.LvlWarn
	case analog.Debug:
		return log.LvlDebug
	case analog.Info:
		return log.LvlInfo
	case analog.Warning:
		return log.LvlWarn
	case analog.Error:
		return log.LvlError
	case analog.Critical:
		return log.LvlCrit
	case analog.Disabled:
		panic(level)
		// This should never occur. Maybe log that a message leaked?
		//nolint
		return log.LvlError
	default:
		// This shouldn't happen...
		panic(level)
	}
}

type noopHandler struct{}

func (b noopHandler) Handle(r analog.Record) {
}

type adapterHandler struct{}

func (b adapterHandler) Handle(r analog.Record) {
	// Note that anacrolix/log now partly supports stdlib slog, so if Erigon switches to that, it
	// would be better to use that too. anacrolix/torrent might change eventually too.
	lvl := anacrolixToErigonLogLevel(r.Level)
	msg := "[downloader torrent client] " + string(bytes.TrimSuffix(analog.LineFormatter(r), []byte{'\n'}))
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
