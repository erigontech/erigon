package smt

import "fmt"

func intArrayToString(a []int) string {
	s := ""
	for _, v := range a {
		s += fmt.Sprintf("%d", v)
	}
	return s
}
