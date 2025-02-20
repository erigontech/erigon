package logger

import (
	"sync"
	"sync/atomic"

	"github.com/erigontech/erigon-lib/log/v3"
)

var Logger log.Logger

func SetLogger(logger log.Logger) {
	onceError.Do(func() {
		Logger = logger
	})
}

type OnceError struct {
	once sync.Once
	done atomic.Bool // or sync.Mutex + bool
}

var onceError OnceError

func (o *OnceError) Do(f func()) {
	if o.done.Load() {
		panic("already called")
	}

	o.once.Do(func() {
		f()
		o.done.Store(true)
	})
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
