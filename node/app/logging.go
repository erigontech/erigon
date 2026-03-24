package app

import (
	"fmt"
	"reflect"
	"sync"

	log "github.com/erigontech/erigon/common/log/v3"
)

// Logger extends the standard erigon logger with level control and labels
// needed by the component framework.
type Logger interface {
	log.Logger
	SetLevel(lvl log.Lvl) log.Lvl
	SetLabels(labels ...string)
	TraceEnabled() bool
	DebugEnabled() bool
}

// logger wraps erigon's log.Logger with per-instance level and labels.
type logger struct {
	log.Logger
	mu     sync.RWMutex
	level  log.Lvl
	labels []string
}

// applog is the default package-level logger used when no logger is in context.
var applog Logger = &logger{Logger: log.Root(), level: log.LvlInfo}

func NewLogger(level log.Lvl, ctx []string, kvCtx interface{}) Logger {
	var kv []interface{}
	if kvPairs, ok := kvCtx.([]interface{}); ok && len(kvPairs) > 0 {
		kv = kvPairs
	} else {
		kv = make([]interface{}, 0, len(ctx)*2)
		for _, c := range ctx {
			kv = append(kv, "module", c)
		}
	}
	return &logger{
		Logger: log.New(kv...),
		level:  level,
		labels: ctx,
	}
}

func (l *logger) SetLevel(lvl log.Lvl) log.Lvl {
	l.mu.Lock()
	prev := l.level
	l.level = lvl
	l.mu.Unlock()
	return prev
}

func (l *logger) SetLabels(labels ...string) {
	l.mu.Lock()
	l.labels = labels
	l.mu.Unlock()
}

func (l *logger) TraceEnabled() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level >= log.LvlTrace
}

func (l *logger) DebugEnabled() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.level >= log.LvlDebug
}

// LogInstance returns a human-readable string representation of a value,
// suitable for use in log key-value pairs. It handles nil values safely.
func LogInstance(value interface{}) string {
	if value == nil {
		return "<nil>"
	}
	v := reflect.ValueOf(value)
	if isNil(v) {
		return "<nil>"
	}
	if s, ok := value.(fmt.Stringer); ok {
		return s.String()
	}
	t := v.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return fmt.Sprintf("%s(%p)", t.Name(), value)
}

// isNil returns false for values where nil is not applicable rather than panicking.
func isNil(value reflect.Value) bool {
	k := value.Kind()
	switch k {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer, reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return value.IsNil()
	}
	return false
}
