package tracers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSwap(t *testing.T) {
	vm := JSVMNew()
	vm.PushInt(1)
	vm.PushInt(2)
	vm.Swap(-1, -2)
	assert.Equal(t, 1, vm.GetInt(-1))
	vm.Pop()
	assert.Equal(t, 2, vm.GetInt(-1))
}

func TestPushAndGetInt(t *testing.T) {
	vm := JSVMNew()
	vm.PushInt(123)
	assert.Equal(t, 123, vm.GetInt(-1))
}

func TestPushAndGetString(t *testing.T) {
	vm := JSVMNew()
	vm.PushString("hello")
	assert.Equal(t, "hello", vm.GetString(-1))
}

func TestPushAndGetBuffer(t *testing.T) {
	vm := JSVMNew()
	vm.PushFixedBuffer(1)
	p, s := vm.GetBuffer(-1)
	assert.Equal(t, uint(1), s)
	assert.Equal(t, byte(0), *(*byte)(p))
}

func TestPutAndGetPropString(t *testing.T) {
	vm := JSVMNew()
	objIndex := vm.PushObject()
	vm.PushString("hello")
	vm.PutPropString(objIndex, "x")
	exists := vm.GetPropString(objIndex, "x")
	assert.Equal(t, true, exists)
	x := vm.GetString(-1)
	assert.Equal(t, "hello", x)
}

func TestGetGlobalString(t *testing.T) {
	vm := JSVMNew()
	vm.EvalString("x = 'hello'")
	exists := vm.GetGlobalString("x")
	assert.Equal(t, true, exists)
	x := vm.GetString(-1)
	assert.Equal(t, "hello", x)
}

func TestPutGlobalString(t *testing.T) {
	vm := JSVMNew()
	vm.PushString("hello")
	vm.PutGlobalString("x")
	exists := vm.GetGlobalString("x")
	assert.Equal(t, true, exists)
	x := vm.GetString(-1)
	assert.Equal(t, "hello", x)
}

func TestCall0(t *testing.T) {
	vm := JSVMNew()
	vm.PushGoFunction(func(ctx *JSVM) int {
		ctx.PushInt(123)
		return 1
	})
	vm.Call(0)
	x := vm.GetInt(-1)
	assert.Equal(t, 123, x)
}

func TestCall1(t *testing.T) {
	vm := JSVMNew()
	vm.PushGoFunction(func(ctx *JSVM) int {
		arg := ctx.GetInt(-1)
		ctx.Pop()
		ctx.PushInt(arg + 120)
		return 1
	})
	vm.PushInt(3)
	vm.Call(1)
	x := vm.GetInt(-1)
	assert.Equal(t, 123, x)
}

func TestCallPropWithGoFunction(t *testing.T) {
	vm := JSVMNew()
	vm.PushGlobalGoFunction("f", func(ctx *JSVM) int {
		ctx.PushInt(123)
		return 1
	})
	objIndex := vm.PushGlobalObject()
	vm.PushString("f")
	errCode := vm.PcallProp(objIndex, 0)
	assert.Equal(t, 0, errCode)
	x := vm.GetInt(-1)
	assert.Equal(t, 123, x)
}

func TestCallProp0(t *testing.T) {
	vm := JSVMNew()
	vm.EvalString("function f() { return 'hello' }")
	objIndex := vm.PushGlobalObject()
	vm.PushString("f")
	errCode := vm.PcallProp(objIndex, 0)
	assert.Equal(t, 0, errCode)
	x := vm.GetString(-1)
	assert.Equal(t, "hello", x)
}

func TestCallProp1(t *testing.T) {
	vm := JSVMNew()
	vm.EvalString("function f(s) { return s + '123' }")
	objIndex := vm.PushGlobalObject()
	vm.PushString("f")
	vm.PushString("hello")
	errCode := vm.PcallProp(objIndex, 1)
	assert.Equal(t, 0, errCode)
	x := vm.GetString(-1)
	assert.Equal(t, "hello123", x)
}

func TestCallPropWithObj(t *testing.T) {
	vm := JSVMNew()
	vm.EvalString("function f(opts) { return opts.name + '123' }")
	globalIndex := vm.PushGlobalObject()
	vm.PushString("f")
	optsIndex := vm.PushObject()
	vm.PushString("hello")
	vm.PutPropString(optsIndex, "name")
	errCode := vm.PcallProp(globalIndex, 1)
	assert.Equal(t, 0, errCode)
	x := vm.GetString(-1)
	assert.Equal(t, "hello123", x)
}

func TestCallPropWithJSObj(t *testing.T) {
	vm := JSVMNew()
	vm.EvalString(`
function Options() { }
Options.prototype.name = function () { return 'hello' }
function makeOptions() { return new Options() }
function f(opts) { return opts.name() + '123' }
`)
	globalIndex := vm.PushGlobalObject()
	vm.PushString("f")

	vm.PushString("makeOptions")
	errCode := vm.PcallProp(globalIndex, 0)
	assert.Equal(t, 0, errCode)

	errCode = vm.PcallProp(globalIndex, 1)
	assert.Equal(t, 0, errCode)
	x := vm.GetString(-1)
	assert.Equal(t, "hello123", x)
}

func TestSafeToString(t *testing.T) {
	vm := JSVMNew()
	vm.PushInt(5)
	assert.Equal(t, "5", vm.SafeToString(-1))
}

func TestEval(t *testing.T) {
	vm := JSVMNew()
	vm.PushString("2 + 3")
	vm.Eval()
	x := vm.GetInt(-1)
	assert.Equal(t, 5, x)
}
