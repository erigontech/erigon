// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

package ps

//int array array -> Create array of length int
func array(interpreter *Interpreter) {
	interpreter.Push(make([]Value, interpreter.PopInt()))
}

//array length int -> Return number of elements in array
func lengtharray(interpreter *Interpreter) {
	interpreter.Push(float64(len(interpreter.Pop().([]Value))))
}

//array  index get any -> Return array element indexed by index
func getarray(interpreter *Interpreter) {
	index := interpreter.PopInt()
	array := interpreter.Pop().([]Value)
	interpreter.Push(array[index])
}

//array  index  any put – -> Put any into array at index
func putarray(interpreter *Interpreter) {
	value := interpreter.Pop()
	index := interpreter.PopInt()
	array := interpreter.Pop().([]Value)
	array[index] = value
}

//array  index  count getinterval subarray -> Return subarray of array starting at index for count elements
func getinterval(interpreter *Interpreter) {
	count := interpreter.PopInt()
	index := interpreter.PopInt()
	array := interpreter.Pop().([]Value)
	subarray := make([]Value, count)
	copy(subarray, array[index:index+count])
	interpreter.Push(subarray)
}

//array1  index  array2 putinterval – Replace subarray of array1 starting at index by array2|packedarray2
func putinterval(interpreter *Interpreter) {
	array2 := interpreter.Pop().([]Value)
	index := interpreter.PopInt()
	array1 := interpreter.Pop().([]Value)
	for i, v := range array2 {
		array1[i+index] = v
	}
}

// any0 … anyn−1  array  astore  array
// stores the objects any0 to anyn−1 from the operand stack into array, where n is the length of array
func astore(interpreter *Interpreter) {
	array := interpreter.Pop().([]Value)
	n := len(array)
	for i := 0; i < n; i++ {
		array[i] = interpreter.Pop()
	}
}

//array aload any0 … any-1   array
//Push all elements of array on stack
func aload(interpreter *Interpreter) {
	array := interpreter.Pop().([]Value)
	for _, v := range array {
		interpreter.Push(v)
	}
	interpreter.Push(array)
}

//array  proc forall – Execute proc for each element of array
func forallarray(interpreter *Interpreter) {
	proc := NewProcedure(interpreter.PopProcedureDefinition())
	array := interpreter.Pop().([]Value)
	for _, v := range array {
		interpreter.Push(v)
		proc.Execute(interpreter)
	}
}

var packing bool = false

func currentpacking(interpreter *Interpreter) {
	interpreter.Push(packing)
}
func setpacking(interpreter *Interpreter) {
	packing = interpreter.PopBoolean()
}

func initArrayOperators(interpreter *Interpreter) {
	interpreter.SystemDefine("array", NewOperator(array))
	interpreter.SystemDefine("getinterval", NewOperator(getinterval))
	interpreter.SystemDefine("putinterval", NewOperator(putinterval))
	interpreter.SystemDefine("astore", NewOperator(astore))
	interpreter.SystemDefine("aload", NewOperator(aload))
	interpreter.SystemDefine("currentpacking", NewOperator(currentpacking))
	interpreter.SystemDefine("setpacking", NewOperator(setpacking))

}
