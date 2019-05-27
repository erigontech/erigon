// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

package ps

import (
	"log"
)

//int dict dict -> Create dictionary with capacity for int elements
func dict(interpreter *Interpreter) {
	interpreter.Push(NewDictionary(interpreter.PopInt()))
}

//dict length int -> Return number of entries in dict
func lengthdict(interpreter *Interpreter) {
	dictionary := interpreter.Pop().(Dictionary)
	interpreter.Push(float64(len(dictionary)))
}

//dict maxlength int -> Return current capacity of dict
func maxlength(interpreter *Interpreter) {
	interpreter.Pop()
	interpreter.Push(float64(999999999)) // push arbitrary value
}

//dict begin – -> Push dict on dictionary stack
func begin(interpreter *Interpreter) {
	interpreter.PushDictionary(interpreter.Pop().(Dictionary))
}

//– end – -> Pop current dictionary off dictionary stack
func end(interpreter *Interpreter) {
	interpreter.PopDictionary()
}

//key  value def – -> Associate key and value in current dictionary
func def(interpreter *Interpreter) {
	value := interpreter.Pop()
	name := interpreter.PopName()
	if p, ok := value.(*ProcedureDefinition); ok {
		value = NewProcedure(p)
	}
	interpreter.Define(name, value)
}

//key load value -> Search dictionary stack for key and return associated value
func load(interpreter *Interpreter) {
	name := interpreter.PopName()
	value, _ := interpreter.FindValueInDictionaries(name)
	if value == nil {
		log.Printf("Can't find value %s\n", name)
	}
	interpreter.Push(value)
}

//key  value store – -> Replace topmost deﬁnition of key
func store(interpreter *Interpreter) {
	value := interpreter.Pop()
	key := interpreter.PopName()
	_, dictionary := interpreter.FindValueInDictionaries(key)
	if dictionary != nil {
		dictionary[key] = value
	}
}

//dict  key get any -> Return value associated with key in dict
func getdict(interpreter *Interpreter) {
	key := interpreter.PopName()
	dictionary := interpreter.Pop().(Dictionary)
	interpreter.Push(dictionary[key])
}

//dict  key  value put – -> Associate key with value in dict
func putdict(interpreter *Interpreter) {
	value := interpreter.Pop()
	key := interpreter.PopName()
	dictionary := interpreter.Pop().(Dictionary)
	dictionary[key] = value
}

//dict  key undef – Remove key and its value from dict
func undef(interpreter *Interpreter) {
	key := interpreter.PopName()
	dictionary := interpreter.Pop().(Dictionary)
	dictionary[key] = nil
}

//dict  key known bool -> Test whether key is in dict
func known(interpreter *Interpreter) {
	key := interpreter.PopName()
	dictionary := interpreter.Pop().(Dictionary)
	interpreter.Push(dictionary[key] != nil)
}

//key where (dict  true) or false -> Find dictionary in which key is deﬁned
func where(interpreter *Interpreter) {
	key := interpreter.PopName()
	_, dictionary := interpreter.FindValueInDictionaries(key)
	if dictionary == nil {
		interpreter.Push(false)
	} else {
		interpreter.Push(dictionary)
		interpreter.Push(true)
	}
}

// dict1  dict2 copy dict2 -> Copy contents of dict1 to dict2
func copydict(interpreter *Interpreter) {
	dict2 := interpreter.Pop().(Dictionary)
	dict1 := interpreter.Pop().(Dictionary)
	for key, value := range dict1 {
		dict2[key] = value
	}
	interpreter.Push(dict2)
}

//dict  proc forall – -> Execute proc for each entry in dict
func foralldict(interpreter *Interpreter) {
	proc := NewProcedure(interpreter.PopProcedureDefinition())
	dict := interpreter.Pop().(Dictionary)
	for key, value := range dict {
		interpreter.Push(key)
		interpreter.Push(value)
		proc.Execute(interpreter)
	}
}

//– currentdict dict -> Return current dictionary
func currentdict(interpreter *Interpreter) {
	interpreter.Push(interpreter.PeekDictionary())
}

//– systemdict dict -> Return system dictionary
func systemdict(interpreter *Interpreter) {
	interpreter.Push(interpreter.SystemDictionary())
}

//– userdict dict -> Return writeable dictionary in local VM
func userdict(interpreter *Interpreter) {
	interpreter.Push(interpreter.UserDictionary())
}

//– globaldict dict -> Return writeable dictionary in global VM
func globaldict(interpreter *Interpreter) {
	interpreter.Push(interpreter.UserDictionary())
}

//– statusdict dict -> Return product-dependent dictionary
func statusdict(interpreter *Interpreter) {
	interpreter.Push(interpreter.UserDictionary())
}

//– countdictstack int -> Count elements on dictionary stack
func countdictstack(interpreter *Interpreter) {
	interpreter.Push(float64(interpreter.DictionaryStackSize()))
}

//array dictstack subarray -> Copy dictionary stack into array
func dictstack(interpreter *Interpreter) {
	panic("No yet implemenented")
}

//– cleardictstack – -> Pop all nonpermanent dictionaries off dictionary stack
func cleardictstack(interpreter *Interpreter) {
	interpreter.ClearDictionaries()
}

func initDictionaryOperators(interpreter *Interpreter) {
	interpreter.SystemDefine("dict", NewOperator(dict))
	//interpreter.SystemDefine("length", NewOperator(length)) // already define in operators_conflict.go
	interpreter.SystemDefine("maxlength", NewOperator(maxlength))
	interpreter.SystemDefine("begin", NewOperator(begin))
	interpreter.SystemDefine("end", NewOperator(end))
	interpreter.SystemDefine("def", NewOperator(def))
	interpreter.SystemDefine("load", NewOperator(load))
	interpreter.SystemDefine("store", NewOperator(store))
	//interpreter.SystemDefine("get", NewOperator(get)) // already define in operators_conflict.go
	//interpreter.SystemDefine("put", NewOperator(put)) // already define in operators_conflict.go
	interpreter.SystemDefine("undef", NewOperator(undef))
	interpreter.SystemDefine("known", NewOperator(known))
	interpreter.SystemDefine("where", NewOperator(where))
	//interpreter.SystemDefine("copydict", NewOperator(copydict)) // already define in operators_conflict.go
	//interpreter.SystemDefine("foralldict", NewOperator(foralldict)) // already define in operators_conflict.go
	interpreter.SystemDefine("currentdict", NewOperator(currentdict))
	interpreter.SystemDefine("systemdict", NewOperator(systemdict))
	interpreter.SystemDefine("userdict", NewOperator(userdict))
	interpreter.SystemDefine("globaldict", NewOperator(globaldict))
	interpreter.SystemDefine("statusdict", NewOperator(statusdict))
	interpreter.SystemDefine("countdictstack", NewOperator(countdictstack))
	interpreter.SystemDefine("dictstack", NewOperator(dictstack))
	interpreter.SystemDefine("cleardictstack", NewOperator(cleardictstack))
}
