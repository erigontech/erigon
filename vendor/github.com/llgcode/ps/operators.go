// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

package ps

type OperatorFunc func(interpreter *Interpreter)

type PrimitiveOperator struct {
	f OperatorFunc
}

func NewOperator(f OperatorFunc) *PrimitiveOperator {
	return &PrimitiveOperator{f}
}

func (o *PrimitiveOperator) Execute(interpreter *Interpreter) {
	o.f(interpreter)
}

func save(interpreter *Interpreter) {
	interpreter.Push("VM Snapshot")
}

func restore(interpreter *Interpreter) {
	interpreter.Pop()
}

func initSystemOperators(interpreter *Interpreter) {
	interpreter.SystemDefine("save", NewOperator(save))
	interpreter.SystemDefine("restore", NewOperator(restore))
	initStackOperator(interpreter)
	initMathOperators(interpreter)
	initArrayOperators(interpreter)
	initDictionaryOperators(interpreter)
	initRelationalOperators(interpreter)
	initControlOperators(interpreter)
	initMiscellaneousOperators(interpreter)
	initDrawingOperators(interpreter)

	initConflictingOperators(interpreter)
}
