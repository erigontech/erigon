package util

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/go-errors/errors"
)

var (
	ErrUnexpectedType = errors.New("Unexpected type")
	ErrToDo           = errors.New("TODO")
)

type DescriptiveErrorSetter func(descriptive bool)

var descriptiveErrorSetters = map[string]DescriptiveErrorSetter{}

func RegisterDescriptiveErrorSetter(packageName string, setter DescriptiveErrorSetter) {
	descriptiveErrorSetters[packageName] = setter
}

// Sets the descriptive errors on or off of a registered package.
func DescriptiveErrors(packageName string, descriptive bool) {
	if setter, ok := descriptiveErrorSetters[packageName]; ok {
		setter(descriptive)
	}
}

// Parses a string formatted like this: `package:true|false` and uses the
// parsed values to call `DescriptiveErrors`.  The value is case insensitive.

func ParseDescriptiveErrors(values []string) {

	for _, value := range values {
		parts := strings.Split(value, ":")

		if len(parts) > 1 {
			switch strings.ToLower(parts[1]) {
			case "true":
				DescriptiveErrors(parts[0], true)
			case "false":
				DescriptiveErrors(parts[0], false)
			}
		}
	}
}

// The maximum number of stackframes on any error.
var MaxStackDepth = 50

// Error is an error with an attached stacktrace. It can be used
// wherever the builtin error interface is expected.
type Error struct {
	Err    error
	stack  []uintptr
	frames []StackFrame
	prefix string
}

// New makes an Error from the given value. If that value is already an
// error then it will be used directly, if not, it will be passed to
// fmt.Errorf("%v"). The stacktrace will point to the line of code that
// called New.
func NewError(e interface{}) *Error {
	var err error

	switch e := e.(type) {
	case error:
		err = e
	default:
		err = fmt.Errorf("%v", e)
	}

	stack := make([]uintptr, MaxStackDepth)
	length := runtime.Callers(2, stack[:])
	return &Error{
		Err:   err,
		stack: stack[:length],
	}
}

// Wrap makes an Error from the given value. If that value is already an
// error then it will be used directly, if not, it will be passed to
// fmt.Errorf("%v"). The skip parameter indicates how far up the stack
// to start the stacktrace. 0 is from the current call, 1 from its caller, etc.
func WrapError(e interface{}, skip int) *Error {
	if e == nil {
		return nil
	}

	var err error

	switch e := e.(type) {
	case *Error:
		return e
	case error:
		err = e
	default:
		err = fmt.Errorf("%v", e)
	}

	stack := make([]uintptr, MaxStackDepth)
	length := runtime.Callers(2+skip, stack[:])
	return &Error{
		Err:   err,
		stack: stack[:length],
	}
}

// WrapPrefix makes an Error from the given value. If that value is already an
// error then it will be used directly, if not, it will be passed to
// fmt.Errorf("%v"). The prefix parameter is used to add a prefix to the
// error message when calling Error(). The skip parameter indicates how far
// up the stack to start the stacktrace. 0 is from the current call,
// 1 from its caller, etc.
func WrapPrefix(e interface{}, prefix string, skip int) *Error {
	if e == nil {
		return nil
	}

	err := WrapError(e, 1+skip)

	if err.prefix != "" {
		prefix = fmt.Sprintf("%s: %s", prefix, err.prefix)
	}

	return &Error{
		Err:    err.Err,
		stack:  err.stack,
		prefix: prefix,
	}

}

// Errorf creates a new error with the given message. You can use it
// as a drop-in replacement for fmt.Errorf() to provide descriptive
// errors in return values.
func Errorf(format string, a ...interface{}) *Error {
	return WrapError(fmt.Errorf(format, a...), 1)
}

// Error returns the underlying error's message.
func (err *Error) Error() string {

	msg := err.Err.Error()
	if err.prefix != "" {
		msg = fmt.Sprintf("%s: %s", err.prefix, msg)
	}

	return msg
}

func (err *Error) Is(original error) bool {

	if errors.Is(err.Err, original) {
		return true
	}

	if original, ok := original.(*Error); ok {
		return err.Is(original.Err)
	}

	return false
}

// Stack returns the callstack formatted the same way that go does
// in runtime/debug.Stack()
func (err *Error) Stack() []byte {
	buf := bytes.Buffer{}

	for _, frame := range err.StackFrames() {
		buf.WriteString(frame.String())
	}

	return buf.Bytes()
}

// Callers satisfies the bugsnag ErrorWithCallerS() interface
// so that the stack can be read out.
func (err *Error) Callers() []uintptr {
	return err.stack
}

// ErrorStack returns a string that contains both the
// error message and the callstack.
func (err *Error) ErrorStack() string {
	return err.TypeName() + " " + err.Error() + "\n" + string(err.Stack())
}

// StackFrames returns an array of frames containing information about the
// stack.
func (err *Error) StackFrames() []StackFrame {
	if err.frames == nil {
		err.frames = make([]StackFrame, len(err.stack))

		for i, pc := range err.stack {
			err.frames[i] = NewStackFrame(pc)
		}
	}

	return err.frames
}

// TypeName returns the type this error. e.g. errors.stringError.
func (err *Error) TypeName() string {
	return reflect.TypeOf(err.Err).Name()
}

func UnexpectedTypeError(expected interface{}, actual interface{}, message ...string) error {
	var expectedTypeName string
	var actualTypeName string

	switch expectedType := expected.(type) {
	case reflect.Type:

		if expectedType.Kind() == reflect.Ptr && expectedType.Elem().Kind() == reflect.Interface {
			expectedType = expectedType.Elem()
		}

		expectedTypeName = expectedType.String()

	default:
		if expected == nil {
			expectedTypeName = "<nil>"
		} else {
			expectedType := reflect.TypeOf(expected)

			if expectedType.Kind() == reflect.Ptr && expectedType.Elem().Kind() == reflect.Interface {
				expectedType = expectedType.Elem()
			}

			expectedTypeName = expectedType.String()
		}
	}

	switch actualType := actual.(type) {
	case reflect.Type:

		if actualType.Kind() == reflect.Ptr && actualType.Elem().Kind() == reflect.Interface {
			actualType = actualType.Elem()
		}

		actualTypeName = actualType.String()

	default:
		if actual == nil {
			actualTypeName = "<nil>"
		} else {
			actualType := reflect.TypeOf(actual)

			if actualType.Kind() == reflect.Ptr && actualType.Elem().Kind() == reflect.Interface {
				actualType = actualType.Elem()
			}

			actualTypeName = actualType.String()
		}
	}

	if len(message) > 0 {
		return WrapError(fmt.Errorf("%s: %w: Expected: %s, Got: %s", message[0], ErrUnexpectedType, expectedTypeName, actualTypeName), 1)
	}

	return WrapError(fmt.Errorf("%w: Expected: %s, Got: %s", ErrUnexpectedType, expectedTypeName, actualTypeName), 1)
}

type Errors []error

func (e Errors) Error() string {
	return fmt.Sprintf("Errors encountered: %v", []error(e))
}
