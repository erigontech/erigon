package ps

import (
	"fmt"
	"log"
)

// dictionary copy conflict with stack copy
// type dicriminant
func commonCopy(interpreter *Interpreter) {
	switch v := interpreter.Peek().(type) {
	case float64:
		copystack(interpreter)
	case Dictionary:
		copydict(interpreter)
	default:
		panic(fmt.Sprintf("Not yet implemented: %v copy", v))
	}
}
func commonforall(interpreter *Interpreter) {
	switch v := interpreter.Get(1).(type) {
	case Dictionary:
		foralldict(interpreter)
	case []Value:
		forallarray(interpreter)
	case string:
		panic("Not yet implemented: string proc forall")
	default:
		panic(fmt.Sprintf("Not yet implemented: %v proc forall", v))
	}
}

func length(interpreter *Interpreter) {
	switch v := interpreter.Peek().(type) {
	case Dictionary:
		lengthdict(interpreter)
	case []Value:
		lengtharray(interpreter)
	case string:
		panic("Not yet implemented: string proc forall")
	default:
		panic(fmt.Sprintf("Not yet implemented: %v length", v))
	}
}
func get(interpreter *Interpreter) {
	switch v := interpreter.Get(1).(type) {
	case Dictionary:
		getdict(interpreter)
	case []Value:
		getarray(interpreter)
	case string:
		panic("Not yet implemented: string proc forall")
	default:
		panic(fmt.Sprintf("Not yet implemented: %v index get", v))
	}
}
func put(interpreter *Interpreter) {
	switch v := interpreter.Get(2).(type) {
	case Dictionary:
		putdict(interpreter)
	case []Value:
		putarray(interpreter)
	case string:
		panic("Not yet implemented: string proc forall")
	default:
		panic(fmt.Sprintf("Not yet implemented: %v index any put", v))
	}
}

func readonly(interpreter *Interpreter) {
	log.Println("readonly, not yet implemented")
}

func cvlit(interpreter *Interpreter) {
	log.Println("cvlit, not yet implemented")
}

func xcheck(interpreter *Interpreter) {
	value := interpreter.Pop()
	if _, ok := value.(*ProcedureDefinition); ok {
		interpreter.Push(true)
	} else {
		interpreter.Push(false)
	}
}

func initConflictingOperators(interpreter *Interpreter) {
	interpreter.SystemDefine("copy", NewOperator(commonCopy))
	interpreter.SystemDefine("forall", NewOperator(commonforall))
	interpreter.SystemDefine("length", NewOperator(length))
	interpreter.SystemDefine("get", NewOperator(get))
	interpreter.SystemDefine("put", NewOperator(put))
	interpreter.SystemDefine("readonly", NewOperator(readonly))
	interpreter.SystemDefine("cvlit", NewOperator(cvlit))
	interpreter.SystemDefine("xcheck", NewOperator(xcheck))
}
