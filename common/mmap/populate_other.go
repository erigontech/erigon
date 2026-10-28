//go:build !linux

package mmap

import "unsafe"

var PopulateEnabled = false

func Populate(unsafe.Pointer) {}
