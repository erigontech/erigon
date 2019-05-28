// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

// Miscellaneous Operators
package ps

//proc bind proc Replace operator names in proc with operators; perform idiom recognition
func bind(interpreter *Interpreter) {
	pdef := interpreter.PopProcedureDefinition()
	values := make([]Value, len(pdef.Values))
	for i, value := range pdef.Values {
		if s, ok := value.(string); ok {
			firstChar := s[0]
			if firstChar != '(' && firstChar != '/' {
				v, _ := interpreter.FindValueInDictionaries(s)
				operator, isOperator := v.(Operator)
				if v == nil {
					// log.Printf("Can't find def: %s\n", s)
				}
				if isOperator {
					values[i] = operator
				} else {
					values[i] = value
				}
			} else {
				values[i] = value
			}
		} else {
			values[i] = value
		}
	}
	pdef.Values = values
	interpreter.Push(pdef)
}

func initMiscellaneousOperators(interpreter *Interpreter) {
	interpreter.SystemDefine("bind", NewOperator(bind))
}
