// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

//Operand Stack Manipulation Operators
package ps

//any pop – -> Discard top element
func pop(interpreter *Interpreter) {
	interpreter.Pop()
}

//any1  any2 exch any2  any1 -> Exchange top two elements
func exch(interpreter *Interpreter) {
	value1 := interpreter.Pop()
	value2 := interpreter.Pop()
	interpreter.Push(value1)
	interpreter.Push(value2)
}

//any dup any  any -> Duplicate top element
func dup(interpreter *Interpreter) {
	interpreter.Push(interpreter.Peek())
}

//any1  …  anyn  n copy any1  …  anyn  any1  …  anyn -> Duplicate top n elements
func copystack(interpreter *Interpreter) {
	n := interpreter.PopInt()
	values := interpreter.GetValues(n)
	for _, value := range values {
		interpreter.Push(value)
	}
}

//anyn  …  any0  n index anyn …  any0  anyn -> Duplicate arbitrary element
func index(interpreter *Interpreter) {
	f := interpreter.PopInt()
	interpreter.Push(interpreter.Get(int(f)))
}

//anyn−1  …  any0  n  j roll any(j−1) mod n  …  any0  anyn−1  …  anyj mod n -> Roll n elements up j times
func roll(interpreter *Interpreter) {
	j := interpreter.PopInt()
	n := interpreter.PopInt()
	values := interpreter.PopValues(n)
	j %= n
	for i := 0; i < n; i++ {
		interpreter.Push(values[(n+i-j)%n])
	}
}

//any1  …  anyn clear -> Discard all elements
func clear(interpreter *Interpreter) {
	interpreter.ClearOperands()
}

//any1  …  anyn count any1  …  anyn  n -> Count elements on stack
func count(interpreter *Interpreter) {
	interpreter.Push(interpreter.OperandSize())
}

//Mark
type Mark struct{}

//– mark mark -> Push mark on stack
func mark(interpreter *Interpreter) {
	interpreter.Push(Mark{})
}

//mark  obj 1  …  obj n cleartomark – -> Discard elements down through mark
func cleartomark(interpreter *Interpreter) {
	value := interpreter.Pop()
	for _, ok := value.(Mark); !ok; {
		value = interpreter.Pop()
	}
}

//mark  obj 1  …  obj n counttomark mark  obj 1  …  obj n  n -> Count elements down to mark
func counttomark(interpreter *Interpreter) {
	i := 0
	value := interpreter.Get(i)
	for _, ok := value.(Mark); !ok; i++ {
		value = interpreter.Get(i)
	}
	interpreter.Push(float64(i))
}

func initStackOperator(interpreter *Interpreter) {
	interpreter.SystemDefine("pop", NewOperator(pop))
	interpreter.SystemDefine("exch", NewOperator(exch))
	interpreter.SystemDefine("dup", NewOperator(dup))
	interpreter.SystemDefine("index", NewOperator(index))
	interpreter.SystemDefine("roll", NewOperator(roll))
	interpreter.SystemDefine("clear", NewOperator(clear))
	interpreter.SystemDefine("count", NewOperator(count))
	interpreter.SystemDefine("mark", NewOperator(mark))
	interpreter.SystemDefine("cleartomark", NewOperator(mark))
	interpreter.SystemDefine("counttomark", NewOperator(mark))
}
