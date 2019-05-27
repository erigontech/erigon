// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

package ps

func eq(interpreter *Interpreter) {
	value1 := interpreter.Pop()
	value2 := interpreter.Pop()
	interpreter.Push(value1 == value2)
}

func ne(interpreter *Interpreter) {
	value1 := interpreter.Pop()
	value2 := interpreter.Pop()
	interpreter.Push(value1 != value2)
}

func not(interpreter *Interpreter) {
	b := interpreter.PopBoolean()
	interpreter.Push(!b)
}

func lt(interpreter *Interpreter) {
	f2 := interpreter.PopFloat()
	f1 := interpreter.PopFloat()
	interpreter.Push(f1 < f2)
}
func gt(interpreter *Interpreter) {
	f2 := interpreter.PopFloat()
	f1 := interpreter.PopFloat()
	interpreter.Push(f1 > f2)
}

func initRelationalOperators(interpreter *Interpreter) {
	interpreter.SystemDefine("eq", NewOperator(eq))
	interpreter.SystemDefine("ne", NewOperator(ne))
	interpreter.SystemDefine("not", NewOperator(not))
	interpreter.SystemDefine("lt", NewOperator(lt))
	interpreter.SystemDefine("gt", NewOperator(gt))
}
