// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

package ps

type ProcedureDefinition struct {
	Values []Value
}

func NewProcedureDefinition() *ProcedureDefinition {
	proceduredef := new(ProcedureDefinition)
	proceduredef.Values = make([]Value, 0, 100)
	return proceduredef
}

func (p *ProcedureDefinition) Add(value Value) {
	p.Values = append(p.Values, value)
}

type Procedure struct {
	def *ProcedureDefinition
}

func NewProcedure(def *ProcedureDefinition) *Procedure {
	return &Procedure{def}
}

func (p *Procedure) Execute(interpreter *Interpreter) {
	for _, value := range p.def.Values {
		if s, ok := value.(string); ok {
			firstChar := s[0]
			if firstChar != '(' && firstChar != '/' {
				interpreter.computeReference(s)
			} else {
				interpreter.Push(value)
			}
		} else {
			operator, isOperator := value.(Operator)
			if isOperator {
				operator.Execute(interpreter)
			} else {
				interpreter.Push(value)
			}
		}
	}
}
