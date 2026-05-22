package spectest

import (
	"io/fs"
	"testing"
)

var UnimplementedHandler = HandlerFunc(func(t *testing.T, root fs.FS, c TestCase) (err error) {
	t.Skipf("Handler not implemented: %s/%s", c.RunnerName, c.HandlerName)
	return nil
})
