package util

import (
	"fmt"
	"reflect"
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

/*
type Logger interface {
	// Log a message at the given level with context key/value pairs
	Trace(msg string, ctx ...interface{})
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
	Log(level Lvl, msg string, ctx ...interface{})

	TraceEnabled() bool
	DebugEnabled() bool
	InfoEnabled() bool
	WarnEnabled() bool
	ErrorEnabled() bool
	CritEnabled() bool
	Enabled(level Lvl) bool
}*/

/*
func init() {
	zerolog.ErrorStackMarshaler = MarshalStack

	if logFormat, ok := os.LookupEnv("LOG_FORMAT"); ok { //json,console
		if strings.EqualFold(logFormat, "console") {
			log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		}
	}

	RegisterEnvGetter("util/logging", func() map[string]interface{} {
		if logFormat, ok := os.LookupEnv("LOG_FORMAT"); ok {
			return map[string]interface{}{"LOG_FORMAT": logFormat}
		}

		return map[string]interface{}{"LOG_FORMAT": "json"}
	})
}

type LevelSetter func(level zerolog.Level) *zerolog.Logger
type LevelGetter func() zerolog.Level

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
			b, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
				return
			}

			LogLevels(strings.Split(string(b), ","))

			return
		}

		if r.Method == "GET" {
			var logLevels = []string{fmt.Sprintf("default:%s", zerolog.GlobalLevel())}

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
//   LogLevel(package.Logger, zerolog.DebugLevel)
func LogLevel(logger string, level zerolog.Level) *zerolog.Logger {
	if strings.EqualFold(logger, "default") {
		zerolog.SetGlobalLevel(level)
		return nil
	}

	if updater, ok := levelUpdaters[logger]; ok {
		return updater.setter(level)
	}

	return nil
}

// Parses a string formatted like this: `package:debug` and uses the
// parsed values to call `LogLevel`.  The level value is case insensitive
// so that `debug`, `Debug` or `DEBUG` will all set the debug value.
// A logger named 'default' will set the default global level for the
// process.  A log level of 'default' will set the log level for the
// logger to the current global default level
func ParseLoggerLevel(loggerLevel string) *zerolog.Logger {
	parts := strings.Split(loggerLevel, ":")

	if len(parts) > 1 {
		if strings.EqualFold(parts[1], "default") {
			return LogLevel(parts[0], zerolog.GlobalLevel())
		}

		if level, err := zerolog.ParseLevel(strings.ToLower(parts[1])); err == nil {
			return LogLevel(parts[0], level)
		}
	}

	return nil
}

// Parses each of the passed in level values and sets the logger to the
// specified log level.  Assumes the strings are of format `package:level`
// as used by `ParseLoggerLevel`
func LogLevels(levels []string) []*zerolog.Logger {
	var loggers []*zerolog.Logger

	for _, level := range levels {
		loggers = append(loggers, ParseLoggerLevel(level))
	}

	return loggers
}

type Logger struct {
	*zerolog.Logger
	sync.RWMutex
}
*/
