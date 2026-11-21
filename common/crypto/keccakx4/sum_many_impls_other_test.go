//go:build !amd64 || purego

package keccakx4

func availableX4() map[string]func(*[100]uint64, *[4]*byte) { return nil }
