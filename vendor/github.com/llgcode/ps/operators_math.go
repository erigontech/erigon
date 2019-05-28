// Copyright 2010 The postscript-go Authors. All rights reserved.
// created: 13/12/2010 by Laurent Le Goff

//Arithmetic and Math Operators
package ps

import (
	"math"
	"math/rand"
)

// begin Primitive Operator implementation

//num1  num2 add sum -> Return num1 plus num2
func add(interpreter *Interpreter) {
	num2 := interpreter.PopFloat()
	num1 := interpreter.PopFloat()
	interpreter.Push(num1 + num2)
}

//num1  num2 div quotient -> Return num1 divided by num2
func div(interpreter *Interpreter) {
	num2 := interpreter.PopFloat()
	num1 := interpreter.PopFloat()
	interpreter.Push(num1 / num2)
}

//int1  int2 idiv quotient -> Return int1 divided by int2
func idiv(interpreter *Interpreter) {
	int2 := interpreter.PopInt()
	int1 := interpreter.PopInt()
	interpreter.Push(float64(int1 / int2))
}

//int   int mod remainder -> Return remainder after dividing int  by int
func mod(interpreter *Interpreter) {
	int2 := interpreter.PopInt()
	int1 := interpreter.PopInt()
	interpreter.Push(float64(int1 % int2))
}

//num1  num2 mul product -> Return num1 times num2
func mul(interpreter *Interpreter) {
	num2 := interpreter.PopFloat()
	num1 := interpreter.PopFloat()
	interpreter.Push(num1 * num2)
}

//num1  num2 sub difference -> Return num1 minus num2
func sub(interpreter *Interpreter) {
	num2 := interpreter.PopFloat()
	num1 := interpreter.PopFloat()
	interpreter.Push(num1 - num2)
}

//num1 abs num2 -> Return absolute value of num1
func abs(interpreter *Interpreter) {
	f := interpreter.PopFloat()
	interpreter.Push(math.Abs(f))
}

//num1 neg num2 -> Return negative of num1
func neg(interpreter *Interpreter) {
	f := interpreter.PopFloat()
	interpreter.Push(-f)
}

//num1 ceiling num2 -> Return ceiling of num1
func ceiling(interpreter *Interpreter) {
	f := interpreter.PopFloat()
	interpreter.Push(float64(int(f + 1)))
}

//num1 ﬂoor num2 -> Return ﬂoor of num1
func floor(interpreter *Interpreter) {
	f := interpreter.PopFloat()
	interpreter.Push(math.Floor(f))
}

//num1 round num2 -> Round num1 to nearest integer
func round(interpreter *Interpreter) {
	f := interpreter.PopFloat()
	interpreter.Push(float64(int(f + 0.5)))
}

//num1 truncate num2 -> Remove fractional part of num1
func truncate(interpreter *Interpreter) {
	f := interpreter.PopFloat()
	interpreter.Push(float64(int(f)))
}

//num sqrt real -> Return square root of num
func sqrt(interpreter *Interpreter) {
	f := interpreter.PopFloat()
	interpreter.Push(float64(math.Sqrt(f)))
}

//num  den atan angle -> Return arctangent of num/den in degrees
func atan(interpreter *Interpreter) {
	den := interpreter.PopFloat()
	num := interpreter.PopFloat()
	interpreter.Push(math.Atan2(num, den) * (180.0 / math.Pi))
}

//angle cos real -> Return cosine of angle degrees
func cos(interpreter *Interpreter) {
	a := interpreter.PopFloat() * math.Pi / 180
	interpreter.Push(math.Cos(a))
}

//angle sin real -> Return sine of angle degrees
func sin(interpreter *Interpreter) {
	a := interpreter.PopFloat() * math.Pi / 180
	interpreter.Push(math.Sin(a))
}

//base  exponent exp real -> Raise base to exponent power
func exp(interpreter *Interpreter) {
	exponent := interpreter.PopFloat()
	base := interpreter.PopFloat()
	interpreter.Push(math.Pow(base, exponent))
}

//num ln real -> Return natural logarithm (base e)
func ln(interpreter *Interpreter) {
	num := interpreter.PopFloat()
	interpreter.Push(math.Log(num))
}

//num log real -> Return common logarithm (base 10)
func log10(interpreter *Interpreter) {
	num := interpreter.PopFloat()
	interpreter.Push(math.Log10(num))
}

//– rand int Generate pseudo-random integer
func randInt(interpreter *Interpreter) {
	interpreter.Push(float64(rand.Int()))
}

var randGenerator *rand.Rand

//int srand – -> Set random number seed
func srand(interpreter *Interpreter) {
	randGenerator = rand.New(rand.NewSource(int64(interpreter.PopInt())))
}

//– rrand int -> Return random number seed
func rrand(interpreter *Interpreter) {
	interpreter.Push(float64(randGenerator.Int()))
}

func initMathOperators(interpreter *Interpreter) {
	interpreter.SystemDefine("add", NewOperator(add))
	interpreter.SystemDefine("div", NewOperator(div))
	interpreter.SystemDefine("idiv", NewOperator(idiv))
	interpreter.SystemDefine("mod", NewOperator(mod))
	interpreter.SystemDefine("mul", NewOperator(mul))
	interpreter.SystemDefine("sub", NewOperator(sub))
	interpreter.SystemDefine("abs", NewOperator(abs))
	interpreter.SystemDefine("neg", NewOperator(neg))
	interpreter.SystemDefine("ceiling", NewOperator(ceiling))
	interpreter.SystemDefine("floor", NewOperator(floor))
	interpreter.SystemDefine("round", NewOperator(round))
	interpreter.SystemDefine("truncate", NewOperator(truncate))
	interpreter.SystemDefine("sqrt", NewOperator(sqrt))
	interpreter.SystemDefine("atan", NewOperator(atan))
	interpreter.SystemDefine("cos", NewOperator(cos))
	interpreter.SystemDefine("sin", NewOperator(sin))
	interpreter.SystemDefine("exp", NewOperator(exp))
	interpreter.SystemDefine("ln", NewOperator(ln))
	interpreter.SystemDefine("log", NewOperator(log10))
	interpreter.SystemDefine("rand", NewOperator(randInt))
	interpreter.SystemDefine("srand", NewOperator(srand))
	interpreter.SystemDefine("rrand", NewOperator(rrand))

}
