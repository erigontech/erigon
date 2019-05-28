package exception

import (
	"errors"
	"fmt"
	"testing"

	"strings"

	"github.com/blendlabs/go-assert"
)

func TestNew(t *testing.T) {
	a := assert.New(t)
	ex := New("this is a test")
	a.Equal("this is a test", fmt.Sprintf("%v", ex))
	a.NotNil(ex.StackTrace())
	a.Nil(ex.Inner())
}

func TestError(t *testing.T) {
	a := assert.New(t)

	ex := As(New("this is a test"))
	message := ex.Error()
	a.NotEmpty(message)
}

func TestNewf(t *testing.T) {
	a := assert.New(t)
	ex := Newf("default_class").WithMessagef("%s", "this is a test")
	a.Equal("default_class\nmessage: this is a test", fmt.Sprintf("%v", ex))
	a.Equal("this is a test", fmt.Sprintf("%m", ex))
	a.NotEmpty(ex.StackTrace())
	a.Nil(ex.Inner())
}

func TestWrapWithError(t *testing.T) {
	a := assert.New(t)

	err := errors.New("This is an error")

	wrappedErr := Wrap(err)
	a.NotNil(wrappedErr)
	typedWrapped := As(wrappedErr)
	a.NotNil(typedWrapped)
	a.Equal("This is an error", fmt.Sprintf("%v", typedWrapped))
}

func TestWrapWithException(t *testing.T) {
	a := assert.New(t)
	ex := New("This is an exception")
	wrappedEx := Wrap(ex)
	a.NotNil(wrappedEx)
	typedWrappedEx := As(wrappedEx)
	a.Equal("This is an exception", fmt.Sprintf("%v", typedWrappedEx))
	a.Equal(ex, typedWrappedEx)
}

func TestWrapWithNil(t *testing.T) {
	a := assert.New(t)

	shouldBeNil := Wrap(nil)
	a.Nil(shouldBeNil)
	a.Equal(nil, shouldBeNil)
}

func TestWrapWithTypedNil(t *testing.T) {
	a := assert.New(t)

	var nilError error
	a.Nil(nilError)
	a.Equal(nil, nilError)

	shouldBeNil := Wrap(nilError)
	a.Nil(shouldBeNil)
	a.True(shouldBeNil == nil)
}

func TestWrapWithReturnedNil(t *testing.T) {
	a := assert.New(t)

	returnsNil := func() error {
		return nil
	}

	shouldBeNil := Wrap(returnsNil())
	a.Nil(shouldBeNil)
	a.True(shouldBeNil == nil)

	returnsTypedNil := func() error {
		return Wrap(nil)
	}

	shouldAlsoBeNil := returnsTypedNil()
	a.Nil(shouldAlsoBeNil)
	a.True(shouldAlsoBeNil == nil)
}

func TestCallers(t *testing.T) {
	a := assert.New(t)

	callStack := func() *stack { return callers() }()

	a.NotNil(callStack)
	callstackStr := fmt.Sprintf("%+v", callStack)
	a.True(strings.Contains(callstackStr, "testing.tRunner"), callstackStr)
}

func TestExceptionFormatters(t *testing.T) {
	assert := assert.New(t)

	// test the "%v" formatter with just the exception class.
	class := &Ex{class: "this is a test"}
	assert.Equal("this is a test", fmt.Sprintf("%v", class))

	classAndMessage := &Ex{class: "foo", message: "bar"}
	assert.Equal("foo\nmessage: bar", fmt.Sprintf("%v", classAndMessage))

	message := &Ex{message: "bar"}
	assert.Equal("bar", fmt.Sprintf("%v", message))
}

func TestNestWithCycle(t *testing.T) {
	a := assert.New(t)

	ex1 := New("This is an error")
	err := Nest(ex1, ex1)

	a.NotNil(err)
	a.NotEmpty(err.Error())

	typedException := As(err)
	a.Equal(ex1, typedException)
}

func TestNestNil(t *testing.T) {
	a := assert.New(t)

	var ex1 error
	var ex2 error
	var ex3 error

	err := Nest(ex1, ex2, ex3)
	a.Nil(err)
	a.Equal(nil, err)
}
