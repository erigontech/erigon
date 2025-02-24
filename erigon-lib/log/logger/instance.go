package logger

import (
	"sync"

	"github.com/erigontech/erigon-lib/log/v3"
)

var Logger log.Logger

func SetLogger(logger log.Logger) {
	onceExec.Do(func() {
		Logger = logger
	})
}

type OnceExec struct {
	done       sync.Mutex
	statusDone bool
}

var onceExec OnceExec

func (o *OnceExec) Do(f func()) {
	o.done.Lock()
	defer o.done.Unlock()
	if o.statusDone {
		panic("already called")
	}
	o.statusDone = true
	f()
}

func Trace(msg string, ctx ...interface{}) {
	Logger.Log(log.LvlTrace, msg, ctx)
}

func Debug(msg string, ctx ...interface{}) {
	Logger.Log(log.LvlDebug, msg, ctx)
}

func Info(msg string, ctx ...interface{}) {
	Logger.Log(log.LvlInfo, msg, ctx)
}

func Warn(msg string, ctx ...interface{}) {
	Logger.Log(log.LvlWarn, msg, ctx)
}

func Error(msg string, ctx ...interface{}) {
	Logger.Log(log.LvlError, msg, ctx)
}

func Crit(msg string, ctx ...interface{}) {
	Logger.Log(log.LvlCrit, msg, ctx)
}
