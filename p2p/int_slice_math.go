package p2p

import (
	"github.com/urfave/cli"
)

type IntSlice cli.IntSlice
type BoolSlice []bool

func (a IntSlice) sub(b IntSlice) IntSlice {
	len_a := len(a)
	if len_a != len(b) {
		panic("Error, operands must have the same lenght!")
	}
	returnValue := make(IntSlice, len_a)
	for i := 0; i < len_a; i++ {
		returnValue[i] = a[i] - b[i]
	}
	return returnValue
}

func (a IntSlice) mul(b int) IntSlice {
	len_a := len(a)
	returnValue := make(IntSlice, len_a)
	for i := 0; i < len_a; i++ {
		returnValue[i] = b * a[i]
	}
	return returnValue
}

func (a IntSlice) smaller_than(b IntSlice) BoolSlice {
	len_a := len(a)
	if len_a != len(b) {
		panic("Error, operands must have the same lenght!")
	}
	returnValue := make(BoolSlice, len_a)
	for i := 0; i < len_a; i++ {
		returnValue[i] = a[i] < b[i]
	}
	return returnValue
}

func (a IntSlice) larger_than_or_equal(b IntSlice) BoolSlice {
	len_a := len(a)
	if len_a != len(b) {
		panic("Error, operands must have the same lenght!")
	}
	returnValue := make(BoolSlice, len_a)
	for i := 0; i < len_a; i++ {
		returnValue[i] = a[i] >= b[i]
	}
	return returnValue
}

func (a IntSlice) larger_than(b IntSlice) BoolSlice {
	len_a := len(a)
	if len_a != len(b) {
		panic("Error, operands must have the same lenght!")
	}
	returnValue := make(BoolSlice, len_a)
	for i := 0; i < len_a; i++ {
		returnValue[i] = a[i] > b[i]
	}
	return returnValue
}

func (a IntSlice) saturate(b IntSlice) IntSlice {
	isLargerArray := a.larger_than(b)
	for i := 0; i < len(isLargerArray); i++ {
		if isLargerArray[i] {
			a[i] = b[i]
		}
	}
	return a
}

func (a BoolSlice) all(signal bool) bool {
	for i := 0; i < len(a); i++ {
		if a[i] == !signal {
			return false
		}
	}
	return true
}

func (a BoolSlice) any(signal bool) bool {
	for i := 0; i < len(a); i++ {
		if a[i] == signal {
			return true
		}
	}
	return false
}
