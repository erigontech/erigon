package main

import (
	"strings"
)

func getPkgDir(path string) string {
	r := strings.Split(path, "/")
	if len(r) < 4 {
		panic("expected at least len 4")
	}
	if r[0] != "github.com" {
		panic("expected github.com to be the first item")
	}
	result := "./" + r[4]
	if len(r) > 4 {
		for i := 4; i < len(r); i++ {
			result += ("/" + r[i])
		}
	}
	return result
}
