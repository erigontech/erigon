// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

package ps

import (
	"io"
	"log"
	"os"
	"strconv"

	"github.com/llgcode/draw2d"
)

type Interpreter struct {
	valueStack      ValueStack
	dictionaryStack DictionaryStack
	gc              draw2d.GraphicContext
}

type Value interface{}

type ValueStack []Value

type Dictionary map[string]Value

type DictionaryStack []Dictionary

type Operator interface {
	Execute(interpreter *Interpreter)
}

func NewInterpreter(gc draw2d.GraphicContext) *Interpreter {
	interpreter := new(Interpreter)
	interpreter.valueStack = make([]Value, 0, 100)
	interpreter.dictionaryStack = make([]Dictionary, 2, 10)
	interpreter.dictionaryStack[0] = NewDictionary(100) // System dictionary
	interpreter.dictionaryStack[1] = NewDictionary(100) // user dictionary
	initSystemOperators(interpreter)
	interpreter.gc = gc
	return interpreter
}

func NewDictionary(prealloc int) Dictionary {
	return make(Dictionary, prealloc)
}

func (interpreter *Interpreter) SetGraphicContext(gc draw2d.GraphicContext) {
	interpreter.gc = gc
}

func (interpreter *Interpreter) GetGraphicContext() draw2d.GraphicContext {
	return interpreter.gc
}
func (interpreter *Interpreter) Execute(reader io.Reader) {
	var scanner Scanner
	scanner.Init(reader)
	token := scanner.Scan()
	for token != EOF {
		interpreter.scan(&scanner, token)
		token = scanner.Scan()
	}
}

func (interpreter *Interpreter) ExecuteFile(filePath string) error {
	src, err := os.Open(filePath)
	if src == nil {
		log.Printf("can't open file; err=%s\n", err.Error())
		return err
	}
	defer src.Close()
	interpreter.Execute(src)
	return nil
}
func (interpreter *Interpreter) computeReference(ref string) {
	value, _ := interpreter.FindValueInDictionaries(ref)
	if value == nil {
		log.Printf("Unknown def: %s\n", ref)
	} else {
		operator, isOperator := value.(Operator)
		if isOperator {
			operator.Execute(interpreter)
		} else {
			interpreter.Push(value)
		}
	}
}
func (interpreter *Interpreter) scan(scanner *Scanner, token int) {
	if token == Ident {
		switch scanner.TokenText() {
		case "true":
			interpreter.Push(true)
		case "false":
			interpreter.Push(false)
		case "null":
			interpreter.Push(nil)
		default:
			interpreter.computeReference(scanner.TokenText())
		}
	} else if token == '/' {
		scanner.Scan()
		interpreter.Push("/" + scanner.TokenText())
	} else if token == '[' {
		interpreter.Push(interpreter.scanArray(scanner))
	} else if token == '{' {
		// procedure
		interpreter.Push(interpreter.scanProcedure(scanner))
	} else if token == Float || token == Int {
		f, err := strconv.ParseFloat(scanner.TokenText(), 64)
		if err != nil {
			log.Printf("Float expected: %s\n", scanner.TokenText())
			interpreter.Push(scanner.TokenText())
		} else {
			interpreter.Push(f)
		}
	} else {
		interpreter.Push(scanner.TokenText())
	}
}

func (interpreter *Interpreter) scanArray(scanner *Scanner) []Value {
	array := make([]Value, 0, 10)
	token := scanner.Scan()
	for token != EOF && token != ']' {
		if token == Ident {
			var v Value = scanner.TokenText()
			switch scanner.TokenText() {
			case "true":
				v = true
			case "false":
				v = false
			case "null":
				v = nil
			}
			array = append(array, v)
		} else {
			interpreter.scan(scanner, token)
			array = append(array, interpreter.Pop())
		}
		token = scanner.Scan()
	}
	return array
}

func (interpreter *Interpreter) scanProcedure(scanner *Scanner) *ProcedureDefinition {
	proceduredef := NewProcedureDefinition()
	token := scanner.Scan()
	for token != EOF && token != '}' {
		if token == Ident {
			var v Value = scanner.TokenText()
			switch scanner.TokenText() {
			case "true":
				v = true
			case "false":
				v = false
			case "null":
				v = nil
			}
			proceduredef.Add(v)
		} else {
			interpreter.scan(scanner, token)
			proceduredef.Add(interpreter.Pop())
		}
		token = scanner.Scan()
	}
	return proceduredef
}

//Dictionary Operation

func (interpreter *Interpreter) PushDictionary(dictionary Dictionary) {
	interpreter.dictionaryStack = append(interpreter.dictionaryStack, dictionary)
}

func (interpreter *Interpreter) PopDictionary() Dictionary {
	stackPointer := len(interpreter.dictionaryStack) - 1
	dictionary := interpreter.dictionaryStack[stackPointer]
	interpreter.dictionaryStack = interpreter.dictionaryStack[0:stackPointer]
	return dictionary
}

func (interpreter *Interpreter) PeekDictionary() Dictionary {
	stackPointer := len(interpreter.dictionaryStack) - 1
	return interpreter.dictionaryStack[stackPointer]
}
func (interpreter *Interpreter) ClearDictionaries() {
	interpreter.dictionaryStack = interpreter.dictionaryStack[:2]
}

func (interpreter *Interpreter) DictionaryStackSize() int {
	return len(interpreter.dictionaryStack)
}

func (interpreter *Interpreter) FindValue(name string) Value {
	return interpreter.PeekDictionary()[name]
}

func (interpreter *Interpreter) FindValueInDictionaries(name string) (Value, Dictionary) {
	for i := len(interpreter.dictionaryStack) - 1; i >= 0; i-- {
		value := interpreter.dictionaryStack[i][name]
		if value != nil {
			return value, interpreter.dictionaryStack[i]
		}
	}
	return nil, nil
}

func (interpreter *Interpreter) UserDictionary() Dictionary {
	return interpreter.dictionaryStack[0]
}

func (interpreter *Interpreter) SystemDictionary() Dictionary {
	return interpreter.dictionaryStack[0]
}

func (interpreter *Interpreter) Define(name string, value Value) {
	interpreter.PeekDictionary()[name] = value
}

func (interpreter *Interpreter) SystemDefine(name string, value Value) {
	interpreter.dictionaryStack[0][name] = value
}

//Operand Operation

func (interpreter *Interpreter) Push(operand Value) {
	//log.Printf("Push operand: %v\n", operand)
	interpreter.valueStack = append(interpreter.valueStack, operand)
}

func (interpreter *Interpreter) Pop() Value {
	valueStackPointer := len(interpreter.valueStack) - 1
	operand := interpreter.valueStack[valueStackPointer]
	interpreter.valueStack = interpreter.valueStack[0:valueStackPointer]
	//log.Printf("Pop operand: %v\n", operand)
	return operand
}

func (interpreter *Interpreter) PopValues(n int) []Value {
	valueStackPointer := len(interpreter.valueStack) - 1
	operands := make([]Value, n)
	copy(operands, interpreter.valueStack[valueStackPointer-n+1:valueStackPointer+1])
	interpreter.valueStack = interpreter.valueStack[0 : valueStackPointer-n+1]
	return operands
}

func (interpreter *Interpreter) GetValues(n int) []Value {
	valueStackPointer := len(interpreter.valueStack) - 1
	operands := make([]Value, n)
	copy(operands, interpreter.valueStack[valueStackPointer-n+1:valueStackPointer+1])
	return operands
}

func (interpreter *Interpreter) Get(index int) Value {
	valueStackPointer := len(interpreter.valueStack) - 1
	return interpreter.valueStack[valueStackPointer-index]
}

func (interpreter *Interpreter) Peek() Value {
	valueStackPointer := len(interpreter.valueStack) - 1
	return interpreter.valueStack[valueStackPointer]
}

func (interpreter *Interpreter) OperandSize() int {
	return len(interpreter.valueStack)
}

func (interpreter *Interpreter) ClearOperands() {
	interpreter.valueStack = interpreter.valueStack[0:0]
}

// misc pop

func (interpreter *Interpreter) PopFloat() float64 {
	operand := interpreter.Pop()
	return operand.(float64)
}

func (interpreter *Interpreter) PopInt() int {
	f := interpreter.PopFloat()
	return int(f)
}

func (interpreter *Interpreter) PopOperator() Operator {
	operator := interpreter.Pop()
	return operator.(Operator)
}

func (interpreter *Interpreter) PopProcedureDefinition() *ProcedureDefinition {
	def := interpreter.Pop()
	return def.(*ProcedureDefinition)
}

func (interpreter *Interpreter) PopName() string {
	name := interpreter.Pop().(string)
	return name[1:]
}

func (interpreter *Interpreter) PopString() string {
	s := interpreter.Pop().(string)
	return s[1 : len(s)-1]
}

func (interpreter *Interpreter) PopBoolean() bool {
	s := interpreter.Pop()
	return s.(bool)
}

func (interpreter *Interpreter) PopArray() []Value {
	s := interpreter.Pop()
	return s.([]Value)
}
