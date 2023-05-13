package spectest

import (
	"io/fs"
	"testing"
)

type HandlerFunc func(t *testing.T, root fs.FS, c TestCase) (err error)

func (h HandlerFunc) Run(t *testing.T, root fs.FS, c TestCase) (err error) {
	return h(t, root, c)
}

type Handler interface {
	Run(t *testing.T, root fs.FS, c TestCase) (err error)
}
