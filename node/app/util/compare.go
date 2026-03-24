package util

import (
	"bytes"
	"reflect"
)

type Equatable interface {
	Equals(other interface{}) bool
}

// Comparable implementers provide a comparable method to
// compare with other instances which implement this
//
// Should return a number:
//
//	negative , if this < other
//	zero     , if this == other
//	positive , if this > other
type Comparable interface {
	CompareTo(other interface{}) int
}

// Equal checks two values are equal via the Equatable or
// Comparable interfaces available - otherwise falls back to ==
func Equal(a, b interface{}) bool {
	if ea, ok := a.(Equatable); ok {
		return ea.Equals(b)
	} else if ca, ok := a.(Comparable); ok {
		return ca.CompareTo(b) == 0
	} else if eb, ok := b.(Equatable); ok {
		return eb.Equals(a)
	} else if cb, ok := b.(Comparable); ok {
		return cb.CompareTo(a) == 0
	}

	return a == b
}

type Comparator func(a, b interface{}) int

// CompoundComparator is an array of comparators which provide comparison of an
// array of values
type CompoundComparator []Comparator

//	Compare compares two objects via  CompareTo if they
//
// both implement Comparable, otherwise if one is non comparable
// a type comparison is undertaken otherwise if neither are comparable
// a Pointer comparison is undertaken
func Compare(a, b interface{}) int {
	if a == nil {
		if b == nil {
			return 0
		}

		return -1
	} else {
		if b == nil {
			return 1
		}
	}

	if acomp, ok := a.(Comparable); ok {
		if bcomp, ok := b.(Comparable); ok {
			return acomp.CompareTo(bcomp)
		} else {
			if reflect.ValueOf(reflect.TypeOf(a)).Pointer()-reflect.ValueOf(reflect.TypeOf(b)).Pointer() > 0 {
				return 1
			}

			return -1
		}
	} else {
		if _, ok := b.(Comparable); ok {
			if reflect.ValueOf(reflect.TypeOf(a)).Pointer()-reflect.ValueOf(reflect.TypeOf(b)).Pointer() > 0 {
				return 1
			}

			return -1
		}
	}

	if _, ok := a.(string); ok {
		if _, ok := b.(string); ok {
			return StringComparator(a, b)
		}
	}

	pa := reflect.ValueOf(a).Pointer()
	pb := reflect.ValueOf(b).Pointer()

	if pa-pb != 0 {
		if (pa - pb) > 0 {
			return 1
		}

		return -1
	}

	return 0
}

func CompoundCompare(a, b interface{}) int {
	avals := a.([]interface{})
	bvals := b.([]interface{})

	if len(avals) >= len(bvals) {
		lenb := len(bvals)
		for index, aval := range avals {
			if index == lenb {
				return 1
			}

			res := Compare(aval, bvals[index])

			if res != 0 {
				return res
			}
		}
	} else {
		lena := len(avals)
		for index, bval := range bvals {
			if index == lena {
				return -1
			}

			res := Compare(avals[index], bval)

			if res != 0 {
				return res
			}
		}
	}

	return 0
}

// BytesComparator provides a basic comparison on []byte
func BytesComparator(a, b interface{}) int {
	bytesa := a.([]byte)
	bytesb := b.([]byte)
	return bytes.Compare(bytesa, bytesb)
}

// BytesComparator provides a basic comparison on arrays of []byte
func BytesArrayComparator(a, b interface{}) int {
	b1 := a.([][]byte)
	b2 := b.([][]byte)

	min := len(b2)
	if len(b1) < len(b2) {
		min = len(b1)
	}
	diff := 0
	for i := 0; i < min && diff == 0; i++ {
		diff = bytes.Compare(b1[i], b2[i])
	}
	if diff == 0 {
		diff = len(b1) - len(b2)
	}
	if diff < 0 {
		return -1
	}
	if diff > 0 {
		return 1
	}
	return 0
}

//	Compare implements the comparator function for a list of values
//
// passed as an array of interfaces
func (comparators CompoundComparator) Compare(a, b interface{}) int {
	var avals []interface{}
	var bvals []interface{}

	avals = a.([]interface{})
	bvals = b.([]interface{})

	//fmt.Printf("A=%v, b=%v\n", a, b)
	if len(avals) >= len(bvals) {
		lenb := len(bvals)
		for index, aval := range avals {
			if index == lenb {
				return 1
			}

			res := comparators[index](aval, bvals[index])

			if res != 0 {
				return res
			}
		}
	} else {
		lena := len(avals)
		for index, bval := range bvals {
			if index == lena {
				return -1
			}

			res := comparators[index](avals[index], bval)

			if res != 0 {
				return res
			}
		}
	}

	return 0
}

// StringComparator provides a fast comparison on strings
func StringComparator(a, b interface{}) int {
	s1 := a.(string)
	s2 := b.(string)
	min := len(s2)
	if len(s1) < len(s2) {
		min = len(s1)
	}
	diff := 0
	for i := 0; i < min && diff == 0; i++ {
		diff = int(s1[i]) - int(s2[i])
	}
	if diff == 0 {
		diff = len(s1) - len(s2)
	}
	if diff < 0 {
		return -1
	}
	if diff > 0 {
		return 1
	}
	return 0
}

func StringsComparator(a, b interface{}) int {
	s1 := a.([]string)
	s2 := b.([]string)
	min := len(s2)
	if len(s1) < len(s2) {
		min = len(s1)
	}
	diff := 0
	for i := 0; i < min && diff == 0; i++ {
		diff = StringComparator(s1[i], s2[i])
	}
	if diff == 0 {
		diff = len(s1) - len(s2)
	}
	if diff < 0 {
		return -1
	}
	if diff > 0 {
		return 1
	}
	return 0
}
