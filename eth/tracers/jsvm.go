package tracers

import (
	"github.com/dop251/goja"
	"unsafe"
)

type JSVM struct {
	vm    *goja.Runtime
	stack []goja.Value
}

func JSVMNew() *JSVM {
	return &JSVM{
		vm:    goja.New(),
		stack: make([]goja.Value, 0, 100),
	}
}

func (vm *JSVM) Pop() {
	vm.stack = vm.stack[:len(vm.stack)-1]
}

func (vm *JSVM) Swap(index1 int, index2 int) {
	vm.stack[len(vm.stack)+index1], vm.stack[len(vm.stack)+index2] = vm.stack[len(vm.stack)+index2], vm.stack[len(vm.stack)+index1]
}

func (vm *JSVM) pushAny(val interface{}) {
	vm.stack = append(vm.stack, vm.vm.ToValue(val))
}

func (vm *JSVM) PushBoolean(val bool) {
	vm.pushAny(val)
}

func (vm *JSVM) PushInt(val int) {
	vm.pushAny(val)
}

func (vm *JSVM) PushUint(val uint) {
	vm.pushAny(val)
}

func (vm *JSVM) PushString(val string) string {
	vm.pushAny(val)
	return val
}

func (vm *JSVM) PushFixedBuffer(size int) unsafe.Pointer {
	buf := make([]byte, size)
	vm.pushAny(buf)
	if size == 0 {
		return unsafe.Pointer(nil)
	}
	return unsafe.Pointer(&buf[0])
}

func (vm *JSVM) PushGoFunction(fn0 func(*JSVM) int) {
	fn := func(this goja.Value, args ...goja.Value) (goja.Value, error) {
		vm.stack = append(vm.stack, this)
		vm.stack = append(vm.stack, args...)
		_ = fn0(vm)
		result := vm.stack[len(vm.stack)-1]
		vm.Pop()
		return result, nil
	}
	vm.pushAny(fn)
}

func (vm *JSVM) PushObject() int {
	vm.stack = append(vm.stack, vm.vm.ToValue(vm.vm.NewObject()))
	return len(vm.stack) - 1
}

func (vm *JSVM) PushUndefined() {
	vm.stack = append(vm.stack, goja.Undefined())
}

func (vm *JSVM) GetInt(index int) int {
	return int(vm.stack[len(vm.stack)+index].Export().(int64))
}

func (vm *JSVM) GetString(index int) string {
	return vm.stack[len(vm.stack)+index].Export().(string)
}

func (vm *JSVM) GetBuffer(index int) (rawPtr unsafe.Pointer, outSize uint) {
	v := vm.stack[len(vm.stack)+index]
	expValue := v.Export()

	// toAddress() and some others are passed a string, but try to parse it with GetBuffer
	if _, ok := expValue.(string); ok {
		return nil, 0
	}

	buf := expValue.([]byte)
	if len(buf) == 0 {
		return unsafe.Pointer(nil), 0
	}
	return unsafe.Pointer(&buf[0]), uint(len(buf))
}

func (vm *JSVM) GetPropString(objIndex int, key string) bool {
	obj := vm.stack[objIndex].ToObject(vm.vm)
	v := obj.Get(key)
	vm.stack = append(vm.stack, v)
	return !goja.IsUndefined(v)
}

func (vm *JSVM) PutPropString(objIndex int, key string) {
	v := vm.stack[len(vm.stack)-1]
	vm.Pop()

	obj := vm.stack[objIndex].ToObject(vm.vm)
	err := obj.Set(key, v)
	if err != nil {
		panic(err)
	}
}

func (vm *JSVM) GetGlobalString(key string) bool {
	v := vm.vm.GlobalObject().Get(key)
	vm.stack = append(vm.stack, v)
	return !goja.IsUndefined(v)
}

func (vm *JSVM) PutGlobalString(key string) {
	v := vm.stack[len(vm.stack)-1]
	vm.Pop()

	obj := vm.vm.GlobalObject()
	err := obj.Set(key, v)
	if err != nil {
		panic(err)
	}
}

func (vm *JSVM) PushGlobalGoFunction(name string, fn0 func(*JSVM) int) {
	fn := func(this goja.Value, args ...goja.Value) (goja.Value, error) {
		vm.stack = append(vm.stack, this)
		vm.stack = append(vm.stack, args...)
		_ = fn0(vm)
		result := vm.stack[len(vm.stack)-1]
		vm.Pop()
		return result, nil
	}
	err := vm.vm.GlobalObject().Set(name, goja.Callable(fn))
	if err != nil {
		panic(err)
	}
}

func (vm *JSVM) PushGlobalObject() int {
	vm.stack = append(vm.stack, vm.vm.GlobalObject())
	return len(vm.stack) - 1
}

func (vm *JSVM) Call(numArgs int) {
	if vm.Pcall(numArgs) != 0 {
		err := vm.stack[len(vm.stack)-1]
		vm.Pop()
		panic(err)
	}
}

func (vm *JSVM) Pcall(numArgs int) int {
	fnValue := vm.stack[len(vm.stack)-numArgs-1]
	args := vm.stack[len(vm.stack)-numArgs:]
	vm.stack = vm.stack[:len(vm.stack)-numArgs-1]

	fn, ok := goja.AssertFunction(fnValue)
	if !ok {
		panic("AssertFunction")
	}

	v, err := fn(goja.Undefined(), args...)
	if err != nil {
		vm.stack = append(vm.stack, vm.vm.ToValue(err))
		return 1
	} else {
		vm.stack = append(vm.stack, v)
		return 0
	}
}

func (vm *JSVM) PcallProp(objIndex int, numArgs int) int {
	key := vm.stack[len(vm.stack)-numArgs-1].String()
	args := vm.stack[len(vm.stack)-numArgs:]
	vm.stack = vm.stack[:len(vm.stack)-numArgs-1]

	obj := vm.stack[objIndex].ToObject(vm.vm)
	fnValue := obj.Get(key)

	fn, ok := goja.AssertFunction(fnValue)
	if !ok {
		panic("AssertFunction")
	}

	v, err := fn(obj, args...)
	if err != nil {
		vm.stack = append(vm.stack, vm.vm.ToValue(err))
		return 1
	} else {
		vm.stack = append(vm.stack, v)
		return 0
	}
}

func (vm *JSVM) SafeToString(index int) string {
	v := vm.stack[len(vm.stack)+index]
	return v.ToString().String()
}

func (vm *JSVM) Eval() {
	src := vm.GetString(-1)
	vm.Pop()
	vm.EvalString(src)
}

func (vm *JSVM) EvalString(src string) {
	v, err := vm.vm.RunString(src)
	if err != nil {
		panic(err)
	}
	vm.stack = append(vm.stack, v)
}

func (vm *JSVM) PevalString(src string) error {
	v, err := vm.vm.RunString(src)
	if err != nil {
		vm.stack = append(vm.stack, vm.vm.ToValue(err))
	} else {
		vm.stack = append(vm.stack, v)
	}
	return err
}
