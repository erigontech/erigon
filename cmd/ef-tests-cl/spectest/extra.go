package spectest

import (
	"io/fs"
	"testing"
)

var UnimplementedHandler = HandlerFunc(func(t *testing.T, root fs.FS, c TestCase) (err error) {
	return ErrHandlerNotImplemented(c.RunnerName + "/" + c.HandlerName)
})
