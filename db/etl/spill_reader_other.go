//go:build !linux

package etl

import "os"

var discardSpillRange = func(_ *os.File, _ []byte, _, _ int) error { return nil }
