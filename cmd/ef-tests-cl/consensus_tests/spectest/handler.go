package spectest

import (
	"io/fs"
	"testing"
)

type Handler interface {
	Run(t *testing.T, root fs.FS) (passed bool, err error)
}
