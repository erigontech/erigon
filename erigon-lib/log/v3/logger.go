package log

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-stack/stack"
)

const timeKey = "t"
const lvlKey = "lvl"
const msgKey = "msg"
const errorKey = "LOG15_ERROR"

// Lvl is a type for predefined log levels.
type Lvl int

// List of predefined log Levels
const (
	LvlCrit Lvl = iota
	LvlError
	LvlWarn
	LvlInfo
	LvlDebug
	LvlTrace
)

var defaultLevel Lvl = LvlInfo
var defaultLogger Logger

func SetDefaultLevel(level Lvl) Lvl {
	prev := defaultLevel
	defaultLevel = level
	return prev
}

func GetDefaultLevel() Lvl {
	return defaultLevel
}

func SetDefaultLogger(logger Logger) Logger {
	prev := defaultLogger
	defaultLogger = logger
	return prev
}

func GetDefaultLogger() Logger {
	if defaultLogger == nil {
		return root
	}
	return defaultLogger
}

// Returns the name of a Lvl
func (l Lvl) String() string {
	switch l {
	case LvlTrace:
		return "trace"
	case LvlDebug:
		return "dbug"
	case LvlInfo:
		return "info"
	case LvlWarn:
		return "warn"
	case LvlError:
		return "eror"
	case LvlCrit:
		return "crit"
	default:
		panic("bad level")
	}
}

// LvlFromString returns the appropriate Lvl from a string name.
// Useful for parsing command line args and configuration files.
func LvlFromString(lvlString string) (Lvl, error) {
	switch lvlString {
	case "trace":
		return LvlTrace, nil
	case "debug", "dbug":
		return LvlDebug, nil
	case "info":
		return LvlInfo, nil
	case "warn":
		return LvlWarn, nil
	case "error", "eror":
		return LvlError, nil
	case "crit":
		return LvlCrit, nil
	default:
		// try to catch e.g. "INFO", "WARN" without slowing down the fast path
		lower := strings.ToLower(lvlString)
		if lower != lvlString {
			return LvlFromString(lower)
		}
		return LvlDebug, fmt.Errorf("log15: unknown level: %v", lvlString)
	}
}

// A Record is what a Logger asks its handler to write
type Record struct {
	Time     time.Time
	Lvl      Lvl
	Msg      string
	Ctx      []any
	Call     stack.Call
	KeyNames RecordKeyNames
}

// RecordKeyNames are the predefined names of the log props used by the Logger interface.
type RecordKeyNames struct {
	Time string
	Msg  string
	Lvl  string
}

// A Logger writes key/value pairs to a Handler
type Logger interface {
	// New returns a new Logger that has this logger's context plus the given context
	New(ctx ...any) Logger

	// GetHandler gets the handler associated with the logger.
	GetHandler() Handler

	// SetHandler updates the logger to write records to the specified handler.
	SetHandler(h Handler)

	// Log a message at the given level with context key/value pairs
	Trace(msg string, ctx ...any)
	Debug(msg string, ctx ...any)
	Info(msg string, ctx ...any)
	Warn(msg string, ctx ...any)
	Error(msg string, ctx ...any)
	Crit(msg string, ctx ...any)
	Log(level Lvl, msg string, ctx ...any)
}

type logger struct {
	ctx []any
	h   *swapHandler
}

func (l *logger) write(msg string, lvl Lvl, depth int, ctx []any) {
	l.h.Log(&Record{
		Time: time.Now(),
		Lvl:  lvl,
		Msg:  msg,
		Ctx:  newContext(l.ctx, ctx),
		Call: stack.Caller(depth),
		KeyNames: RecordKeyNames{
			Time: timeKey,
			Msg:  msgKey,
			Lvl:  lvlKey,
		},
	})
}

func (l *logger) New(ctx ...any) Logger {
	child := &logger{newContext(l.ctx, ctx), new(swapHandler)}
	child.SetHandler(l.h)
	return child
}

func newContext(prefix []any, suffix []any) []any {
	normalizedSuffix := normalize(suffix)
	newCtx := make([]any, len(prefix)+len(normalizedSuffix))
	n := copy(newCtx, prefix)
	copy(newCtx[n:], normalizedSuffix)
	return newCtx
}

func (l *logger) Trace(msg string, ctx ...any) {
	l.write(msg, LvlTrace, 2, ctx)
}

func (l *logger) Debug(msg string, ctx ...any) {
	l.write(msg, LvlDebug, 2, ctx)
}

func (l *logger) Info(msg string, ctx ...any) {
	l.write(msg, LvlInfo, 2, ctx)
}

func (l *logger) Warn(msg string, ctx ...any) {
	l.write(msg, LvlWarn, 2, ctx)
}

func (l *logger) Error(msg string, ctx ...any) {
	l.write(msg, LvlError, 2, ctx)
}

func (l *logger) Crit(msg string, ctx ...any) {
	l.write(msg, LvlCrit, 2, ctx)
}

// Log method to route configurable log level
func (l *logger) Log(level Lvl, msg string, ctx ...any) {
	l.write(msg, level, 2, ctx)
}

func (l *logger) LogAtDepth(depth int, level Lvl, msg string, ctx ...any) {
	l.write(msg, level, depth, ctx)
}

func (l *logger) GetHandler() Handler {
	return l.h.Get()
}

func (l *logger) SetHandler(h Handler) {
	l.h.Swap(h)
}

func normalize(ctx []any) []any {
	// if the caller passed a Ctx object, then expand it

	var normalized []any

	for _, v := range ctx {
		switch v := v.(type) {
		case Ctx:
			normalized = append(normalized, v.toArray()...)
		case CtxPair:
			normalized = append(normalized, v.toArray()...)
		default:
			normalized = append(normalized, v)
		}
	}

	// ctx needs to be even because it's a series of key/value pairs
	// no one wants to check for errors on logging functions,
	// so instead of erroring on bad input, we'll just make sure
	// that things are the right length and users can fix bugs
	// when they see the output looks wrong
	if len(normalized)%2 != 0 {
		normalized = append(ctx, nil, errorKey, "Normalized odd number of arguments by adding nil")
	}

	return normalized
}

// Lazy allows you to defer calculation of a logged value that is expensive
// to compute until it is certain that it must be evaluated with the given filters.
//
// Lazy may also be used in conjunction with a Logger's New() function
// to generate a child logger which always reports the current value of changing
// state.
//
// You may wrap any function which takes no arguments to Lazy. It may return any
// number of values of any type.
type Lazy struct {
	Fn any
}

// Ctx is a map of key/value pairs to pass as context to a log function
// Use this only if you really need greater safety around the arguments you pass
// to the logging functions.
type Ctx map[string]any

func (c Ctx) toArray() []any {
	arr := make([]any, len(c)*2)

	i := 0
	for k, v := range c {
		arr[i] = k
		arr[i+1] = v
		i += 2
	}

	return arr
}

type CtxPair struct {
	key   string
	value any
}

func (p CtxPair) toArray() []any {
	return []any{p.key, p.value}
}

var (
	// CallerFieldName is the field name used for caller field.
	CallerFieldName = "caller"

	// CallerSkipFrameCount is the number of stack frames to skip to find the caller.
	CallerSkipFrameCount = 2

	// CallerMarshalFunc allows customization of global caller marshaling
	CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return file + ":" + strconv.Itoa(line)
	}
)

// Caller adds the file:line of the caller with the zerolog.CallerFieldName key.
// The argument skip is the number of stack frames to ascend
// Skip If not passed, use the global variable CallerSkipFrameCount
func Caller(skip ...int) CtxPair {
	sk := CallerSkipFrameCount
	if len(skip) > 0 {
		sk = skip[0] + CallerSkipFrameCount
	}
	return caller(sk)
}

func caller(skip int) CtxPair {
	ptr, file, line, ok := runtime.Caller(skip)
	if !ok {
		return CtxPair{CallerFieldName, "n/a"}
	}
	return CtxPair{CallerFieldName, CallerMarshalFunc(ptr, file, line)}
}
