package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"reflect"
	"strings"
	"sync"

	"github.com/erigontech/erigon-lib/app/util"
	"github.com/erigontech/erigon-lib/log/v3"
)

// Returns false for values where is nil is
// not applicable rather than panicing
func isNil(value reflect.Value) bool {
	k := value.Kind()
	switch k {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Ptr, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		// Both interface and slice are nil if first word is 0.
		// Both are always bigger than a word; assume flagIndir.
		return value.IsNil()
	default:
		return false
	}

}

func init() {
	RegisterLevelUpdater(path.Base(util.CallerPackageName(1)), applog.SetLevel, func() log.Lvl { return applog.GetLevel() })
}

var applog = NewLogger(log.LvlTrace, []string{path.Base(util.CallerPackageName(1))}, nil)

// LogString returns a string for use in a Str logging field
// without failing is the value passed is nil
func LogString(value interface{}) string {
	if value == nil || isNil(reflect.ValueOf(value)) {
		return "<nil>"
	}

	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprintf("%#v", typed)
	}
}

// LogInstance returns an instance for use in a Str logging field
// it returns <nil> for nil values or the type and instance pointer
// using "%T:%p" formatting
func LogInstance(instance interface{}) string {
	if instance == nil || isNil(reflect.ValueOf(instance)) {
		return "<nil>"
	}

	switch instance := instance.(type) {
	case fmt.Stringer:
		return instance.String()
	case string:
		return instance
	}

	return fmt.Sprintf("%T:%p", instance, instance)
}

func LogId(instance interface{}) string {
	switch instance := instance.(type) {
	case Id:
		return LogString(instance)
	case Identifiable:
		return LogString(instance.Id())
	}

	return "<na>"
}

type Logger interface {
	log.Logger
	TraceEnabled() bool
	DebugEnabled() bool
	InfoEnabled() bool
	WarnEnabled() bool
	ErrorEnabled() bool
	CritEnabled() bool
	Enabled(level log.Lvl) bool

	GetLevel() log.Lvl
	SetLevel(level log.Lvl) log.Lvl

	GetLabels() []string
	SetLabels(parts ...string) []string

	GetCtx() []interface{}
	SetCtx(parts ...interface{}) []interface{}

	Caller() log.CtxPair

	CtxLogger(ctx context.Context) Logger
}

type LevelSetter func(level log.Lvl) log.Lvl
type LevelGetter func() log.Lvl

var levelUpdaters = map[string]*updater{}

type updater struct {
	setter LevelSetter
	getter LevelGetter
}

func RegisterLevelUpdater(logger string, setter LevelSetter, getter LevelGetter) {
	levelUpdaters[logger] = &updater{setter, getter}
}

func LogLevelHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			LogLevels(strings.Split(string(b), ","))

			return
		}

		if r.Method == "GET" {
			var logLevels = []string{fmt.Sprintf("default:%s", log.GetDefaultLevel())}

			for logger, updater := range levelUpdaters {
				level := updater.getter()
				logLevels = append(logLevels, fmt.Sprintf("%s:%s", logger, level))
			}

			_, _ = w.Write([]byte(strings.Join(logLevels, ",")))

			return
		}

		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
	})
}

// Sets the log level of a registered logger to the specified logger.  By
// convention loggers are registered at a package level and specify
// a public Logger variable set to that string so that programmatic log
// setting can be done as follows:
//
//	LogLevel(package.Logger, zerolog.DebugLevel)
func LogLevel(logger string, level log.Lvl) log.Lvl {
	if strings.EqualFold(logger, "default") {
		return log.SetDefaultLevel(level)
	}

	if updater, ok := levelUpdaters[logger]; ok {
		return updater.setter(level)
	}

	return 0
}

// Parses a string formatted like this: `package:debug` and uses the
// parsed values to call `LogLevel`.  The level value is case insensitive
// so that `debug`, `Debug` or `DEBUG` will all set the debug value.
// A logger named 'default' will set the default global level for the
// process.  A log level of 'default' will set the log level for the
// logger to the current global default level
func ParseLoggerLevel(loggerLevel string) log.Lvl {
	parts := strings.Split(loggerLevel, ":")

	if len(parts) > 1 {
		if strings.EqualFold(parts[1], "default") {
			return LogLevel(parts[0], log.GetDefaultLevel())
		}

		if level, err := log.LvlFromString(strings.ToLower(parts[1])); err == nil {
			return LogLevel(parts[0], level)
		}
	}

	return 0
}

// Parses each of the passed in level values and sets the logger to the
// specified log level.  Assumes the strings are of format `package:level`
// as used by `ParseLoggerLevel`
func LogLevels(levelValues []string) []log.Lvl {
	var levels []log.Lvl

	for _, level := range levelValues {
		levels = append(levels, ParseLoggerLevel(level))
	}

	return levels
}

type logger struct {
	sync.RWMutex
	level      log.Lvl
	label      string
	labelParts []string
	ctx        []interface{}
	parent     *logger
	logger     log.Logger
}

func NewLogger(level log.Lvl, labels []string, ctx []interface{}) Logger {
	return &logger{ctx: ctx, level: level, labelParts: labels}
}

func (l *logger) New(ctx ...any) log.Logger {
	return &logger{ctx: ctx, level: l.level, labelParts: l.labelParts, parent: l}
}

func (l *logger) GetHandler() log.Handler {
	if l.logger != nil {
		return l.logger.GetHandler()
	}
	return log.GetDefaultLogger().GetHandler()
}

func (l *logger) SetHandler(h log.Handler) {
	if l.logger == nil {
		l.logger = log.GetDefaultLogger().New()
	}
	l.logger.SetHandler(h)
}

func (l *logger) SetLevel(level log.Lvl) log.Lvl {
	l.Lock()
	defer l.Unlock()
	prev := l.level
	l.level = level
	return prev
}

func (l *logger) GetLevel() log.Lvl {
	l.RLock()
	defer l.RUnlock()
	return l.level
}

func (l *logger) CtxLogger(ctx context.Context) Logger {
	ctxLogger := CtxLogger(ctx)

	if ctxLogger == applog {
		return l
	}

	return &logger{
		level:      ctxLogger.GetLevel(),
		labelParts: ctxLogger.GetLabels(),
		ctx:        ctxLogger.GetCtx(),
		parent:     l,
		logger:     l.logger,
	}
}

func (l *logger) SetLabels(parts ...string) []string {
	l.Lock()
	defer l.Unlock()
	prev := l.labelParts
	l.label = ""
	l.labelParts = parts
	return prev
}

func (l *logger) GetLabels() []string {
	l.RLock()
	defer l.RUnlock()
	return l.labelParts
}

func (l *logger) SetCtx(ctx ...interface{}) []interface{} {
	l.Lock()
	defer l.Unlock()
	prev := l.ctx
	l.ctx = ctx
	return prev
}

func (l *logger) GetCtx() []interface{} {
	l.RLock()
	defer l.RUnlock()
	return l.ctx
}

func (l *logger) Trace(msg string, ctx ...interface{}) {
	l.log(log.LvlTrace, msg, ctx...)
}

func (l *logger) Debug(msg string, ctx ...interface{}) {
	l.log(log.LvlDebug, msg, ctx...)
}

func (l *logger) Info(msg string, ctx ...interface{}) {
	l.log(log.LvlInfo, msg, ctx...)
}

func (l *logger) Warn(msg string, ctx ...interface{}) {
	l.log(log.LvlWarn, msg, ctx...)
}

func (l *logger) Error(msg string, ctx ...interface{}) {
	l.log(log.LvlError, msg, ctx...)
}

func (l *logger) Crit(msg string, ctx ...interface{}) {
	l.log(log.LvlCrit, msg, ctx...)
}

func (l *logger) Log(level log.Lvl, msg string, ctx ...interface{}) {
	l.log(log.LvlCrit, msg, ctx...)
}

func labelParts(l *logger) []string {
	var parentParts []string
	if l.parent != nil {
		parentParts = labelParts(l.parent)
	}
	return append(parentParts, l.labelParts...)
}

func ctxParts(l *logger) []any {
	var parentCtx []any
	if l.parent != nil {
		parentCtx = ctxParts(l.parent)
	}
	return append(parentCtx, l.ctx...)
}

func logProvider(l *logger) log.Logger {
	if logger := l.logger; logger != nil {
		return logger
	}
	if l.parent != nil {
		return logProvider(l.parent)
	}
	return log.GetDefaultLogger()
}

func (l *logger) log(level log.Lvl, msg string, ctx ...interface{}) {
	if l.Enabled(level) {
		l.RLock()
		label := l.label
		labelParts := labelParts(l)
		ctx = append(ctxParts(l), ctx...)
		logger := logProvider(l)
		l.RUnlock()

		if label == "" && len(labelParts) > 0 {
			label = "[" + strings.Join(labelParts, ":") + "]"
			l.Lock()
			l.label = label
			l.Unlock()
		}

		if len(msg) == 0 {
			msg = label
		} else if label != "" {
			msg = strings.Join([]string{label, msg}, " ")
		}

		logger.(interface {
			LogAtDepth(depth int, level log.Lvl, msg string, ctx ...any)
		}).LogAtDepth(4, level, msg, ctx...)
	}
}

func (l *logger) TraceEnabled() bool {
	return l.Enabled(log.LvlTrace)
}
func (l *logger) DebugEnabled() bool {
	return l.Enabled(log.LvlDebug)
}
func (l *logger) InfoEnabled() bool {
	return l.Enabled(log.LvlInfo)
}
func (l *logger) WarnEnabled() bool {
	return l.Enabled(log.LvlWarn)
}
func (l *logger) ErrorEnabled() bool {
	return l.Enabled(log.LvlError)
}
func (l *logger) CritEnabled() bool {
	return l.Enabled(log.LvlCrit)
}
func (l *logger) Enabled(level log.Lvl) bool {
	l.RLock()
	defer l.RUnlock()
	return l.level >= level
}

func (l *logger) Caller() log.CtxPair {
	return log.Caller()
}
