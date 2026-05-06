package log

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	timeFormat     = "2006-01-02T15:04:05-0700"
	termTimeFormat = "01-02|15:04:05.000"
	floatFormat    = 'f'
	termMsgJust    = 40
)

// Format  is the interface implemented by StreamHandler formatters.
type Format interface {
	Format(r *Record) []byte
}

// FormatFunc returns a new Format object which uses
// the given function to perform record formatting.
func FormatFunc(f func(*Record) []byte) Format {
	return formatFunc(f)
}

type formatFunc func(*Record) []byte

func (f formatFunc) Format(r *Record) []byte {
	return f(r)
}

// TerminalFormat formats log records optimized for human readability on
// a terminal with color-coded level output and terser human friendly timestamp.
// This format should only be used for interactive programs or while developing.
//
//	[TIME] [LEVEL] MESSAGE key=value key=value ...
//
// Example:
//
//	[May 16 20:58:45] [DBUG] remove route ns=haproxy addr=127.0.0.1:50002
func TerminalFormat() Format {
	logNoTimestamps := logEnvBool("ERIGON_LOG_NO_TIMESTAMPS", false)
	return FormatFunc(func(r *Record) []byte {
		var color = 0
		switch r.Lvl {
		case LvlCrit:
			color = 35
		case LvlError:
			color = 31
		case LvlWarn:
			color = 33
		case LvlInfo:
			color = 32
		case LvlDebug:
			color = 36
		}

		b := &bytes.Buffer{}
		lvl := r.Lvl.UpperString()
		if logNoTimestamps {
			if color > 0 {
				fmt.Fprintf(b, "\x1b[%dm%s\x1b[0m %s ", color, lvl, r.Msg)
			} else {
				fmt.Fprintf(b, "[%s] %s ", lvl, r.Msg)
			}
		} else {
			if color > 0 {
				fmt.Fprintf(b, "\x1b[%dm%s\x1b[0m[%s] %s ", color, lvl, r.Time.Format(termTimeFormat), r.Msg)
			} else {
				fmt.Fprintf(b, "[%s] [%s] %s ", lvl, r.Time.Format(termTimeFormat), r.Msg)
			}
		}

		// try to justify the log output for short messages
		if len(r.Ctx) > 0 && len(r.Msg) < termMsgJust {
			b.Write(bytes.Repeat([]byte{' '}, termMsgJust-len(r.Msg)))
		}

		// print the keys logfmt style
		logfmt(b, r.Ctx, color)
		return b.Bytes()
	})
}

func TerminalFormatNoColor() Format {
	return FormatFunc(func(r *Record) []byte {
		b := &bytes.Buffer{}
		lvl := r.Lvl.UpperString()
		fmt.Fprintf(b, "[%s] [%s] %s ", lvl, r.Time.Format(termTimeFormat), r.Msg)

		// try to justify the log output for short messages
		if len(r.Ctx) > 0 && len(r.Msg) < termMsgJust {
			b.Write(bytes.Repeat([]byte{' '}, termMsgJust-len(r.Msg)))
		}

		// print the keys logfmt style
		logfmt(b, r.Ctx, 0)
		return b.Bytes()
	})
}

// LogfmtFormat prints records in logfmt format, an easy machine-parseable but human-readable
// format for key/value pairs.
//
// For more details see: http://godoc.org/github.com/kr/logfmt
func LogfmtFormat() Format {
	return FormatFunc(func(r *Record) []byte {
		common := make([]any, 0, 6+len(r.Ctx))
		common = append(common, r.KeyNames.Time, r.Time, r.KeyNames.Lvl, r.Lvl, r.KeyNames.Msg, r.Msg)
		common = append(common, r.Ctx...)
		buf := &bytes.Buffer{}
		logfmt(buf, common, 0)
		return buf.Bytes()
	})
}

func logfmt(buf *bytes.Buffer, ctx []any, color int) {
	// Stack-allocated scratch for encoding numeric values; avoids
	// allocating an intermediate string in the value path.
	var scratch [64]byte
	for i := 0; i < len(ctx); i += 2 {
		if i != 0 {
			buf.WriteByte(' ')
		}

		k, ok := ctx[i].(string)
		v := ctx[i+1]
		if !ok {
			// key is not a string: log errorKey and describe the bad key as value
			k, v = errorKey, ctx[i]
		}

		// XXX: we should probably check that all of your key bytes aren't invalid
		if color > 0 {
			fmt.Fprintf(buf, "\x1b[%dm%s\x1b[0m=", color, k)
		} else {
			buf.WriteString(k)
			buf.WriteByte('=')
		}
		buf.Write(appendLogfmtValue(scratch[:0], v))
	}

	buf.WriteByte('\n')
}

// JsonFormat formats log records as JSON objects separated by newlines.
// It is the equivalent of JsonFormatEx(false, true).
func JsonFormat() Format {
	return JsonFormatEx(false, true)
}

// JsonFormatEx formats log records as JSON objects. If pretty is true,
// records will be pretty-printed. If lineSeparated is true, records
// will be logged with a new line between each record.
func JsonFormatEx(pretty, lineSeparated bool) Format {
	jsonMarshal := json.Marshal
	if pretty {
		jsonMarshal = func(v any) ([]byte, error) {
			return json.MarshalIndent(v, "", "    ")
		}
	}

	return FormatFunc(func(r *Record) []byte {
		props := make(map[string]any)

		props[r.KeyNames.Time] = r.Time
		props[r.KeyNames.Lvl] = r.Lvl.String()
		props[r.KeyNames.Msg] = r.Msg

		for i := 0; i < len(r.Ctx); i += 2 {
			k, ok := r.Ctx[i].(string)
			if !ok {
				props[errorKey] = fmt.Sprintf("%+v is not a string key", r.Ctx[i])
			}
			props[k] = formatJSONValue(r.Ctx[i+1])
		}

		b, err := jsonMarshal(props)
		if err != nil {
			b, _ = jsonMarshal(map[string]string{
				errorKey: err.Error(),
			})
			return b
		}

		if lineSeparated {
			b = append(b, '\n')
		}

		return b
	})
}

func formatShared(value any) (result any) {
	defer func() {
		if err := recover(); err != nil {
			if v := reflect.ValueOf(value); v.Kind() == reflect.Pointer && v.IsNil() {
				result = "nil"
			} else {
				panic(err)
			}
		}
	}()

	switch v := value.(type) {
	case time.Time:
		return v.Format(timeFormat)

	case error:
		return v.Error()

	case fmt.Stringer:
		return v.String()

	default:
		return v
	}
}

func formatJSONValue(value any) any {
	value = formatShared(value)

	switch value.(type) {
	case int, int8, int16, int32, int64, float32, float64, uint, uint8, uint16, uint32, uint64, string:
		return value
	case map[string]any, []any, any:
		return value
	default:
		return fmt.Sprintf("%+v", value)
	}
}

// appendLogfmtValue appends a value in logfmt format to dst and returns the
// extended slice. This is the append-style equivalent of the previous
// formatLogfmtValue+escapeString path: it writes directly into the caller's
// buffer, avoiding an intermediate string allocation and a sync.Pool round-trip.
func appendLogfmtValue(dst []byte, value any) []byte {
	if value == nil {
		return append(dst, "nil"...)
	}
	if t, ok := value.(time.Time); ok {
		// Performance optimization: timeFormat has no escape characters, so
		// we can skip escaping entirely.
		return t.AppendFormat(dst, timeFormat)
	}
	value = formatShared(value)
	switch v := value.(type) {
	case bool:
		return strconv.AppendBool(dst, v)
	case float32:
		return strconv.AppendFloat(dst, float64(v), floatFormat, 3, 64)
	case float64:
		return strconv.AppendFloat(dst, v, floatFormat, 3, 64)
	case int:
		return strconv.AppendInt(dst, int64(v), 10)
	case int8:
		return strconv.AppendInt(dst, int64(v), 10)
	case int16:
		return strconv.AppendInt(dst, int64(v), 10)
	case int32:
		return strconv.AppendInt(dst, int64(v), 10)
	case int64:
		return strconv.AppendInt(dst, v, 10)
	case uint:
		return strconv.AppendUint(dst, uint64(v), 10)
	case uint8:
		return strconv.AppendUint(dst, uint64(v), 10)
	case uint16:
		return strconv.AppendUint(dst, uint64(v), 10)
	case uint32:
		return strconv.AppendUint(dst, uint64(v), 10)
	case uint64:
		return strconv.AppendUint(dst, v, 10)
	case string:
		return appendEscaped(dst, v)
	default:
		return appendEscaped(dst, fmt.Sprintf("%+v", value))
	}
}

// appendEscaped appends s to dst, quoting and escaping if s contains any
// character that would require it in logfmt. Zero-alloc in the fast path
// (s appended verbatim) and one-pass in the slow path.
func appendEscaped(dst []byte, s string) []byte {
	needsQuotes := false
	needsEscape := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c <= ' ' || c == '=' || c == '"' {
			needsQuotes = true
		}
		if c == '\\' || c == '"' || c == '\n' || c == '\r' || c == '\t' {
			needsEscape = true
		}
	}
	if !needsEscape && !needsQuotes {
		return append(dst, s...)
	}
	if needsQuotes {
		dst = append(dst, '"')
	}
	for _, r := range s {
		switch r {
		case '\\', '"':
			dst = append(dst, '\\', byte(r))
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		default:
			dst = utf8.AppendRune(dst, r)
		}
	}
	if needsQuotes {
		dst = append(dst, '"')
	}
	return dst
}

// EnvBool from dbg uses log many times so don't want to make a cycle.
func logEnvBool(envVarName string, defaultVal bool) bool {
	v, _ := os.LookupEnv(envVarName)
	if strings.EqualFold(v, "true") {
		return true
	}
	if strings.EqualFold(v, "false") {
		return false
	}
	return defaultVal
}
