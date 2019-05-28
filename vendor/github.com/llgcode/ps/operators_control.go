// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

package ps

import (
	"log"
)

// any exec – Execute arbitrary object
func exec(interpreter *Interpreter) {
	value := interpreter.Pop()
	if pdef, ok := value.(*ProcedureDefinition); ok {
		NewProcedure(pdef).Execute(interpreter)
	} else if procedure, ok := value.(*Procedure); ok {
		procedure.Execute(interpreter)
	} else {
		log.Printf("Push value: %v\n", value)
		interpreter.Push(value)
	}
}

func ifoperator(interpreter *Interpreter) {
	operator := NewProcedure(interpreter.PopProcedureDefinition())
	condition := interpreter.PopBoolean()
	if condition {
		operator.Execute(interpreter)
	}
}

func ifelse(interpreter *Interpreter) {
	operator2 := NewProcedure(interpreter.PopProcedureDefinition())
	operator1 := NewProcedure(interpreter.PopProcedureDefinition())
	condition := interpreter.PopBoolean()
	if condition {
		operator1.Execute(interpreter)
	} else {
		operator2.Execute(interpreter)
	}
}

func foroperator(interpreter *Interpreter) {
	proc := NewProcedure(interpreter.PopProcedureDefinition())
	limit := interpreter.PopFloat()
	inc := interpreter.PopFloat()
	initial := interpreter.PopFloat()

	for i := initial; i <= limit; i += inc {
		interpreter.Push(i)
		proc.Execute(interpreter)
	}
}

func repeat(interpreter *Interpreter) {
	proc := NewProcedure(interpreter.PopProcedureDefinition())
	times := interpreter.PopInt()
	for i := 0; i <= times; i++ {
		proc.Execute(interpreter)
	}
}

// any stopped bool -> Establish context for catching stop
func stopped(interpreter *Interpreter) {
	value := interpreter.Pop()
	if pdef, ok := value.(*ProcedureDefinition); ok {
		NewProcedure(pdef).Execute(interpreter)
	} else {
		interpreter.Push(value)
	}
	interpreter.Push(false)
}

func initControlOperators(interpreter *Interpreter) {
	interpreter.SystemDefine("exec", NewOperator(exec))
	interpreter.SystemDefine("if", NewOperator(ifoperator))
	interpreter.SystemDefine("ifelse", NewOperator(ifelse))
	interpreter.SystemDefine("for", NewOperator(foroperator))
	interpreter.SystemDefine("repeat", NewOperator(repeat))
	interpreter.SystemDefine("stopped", NewOperator(stopped))
}
