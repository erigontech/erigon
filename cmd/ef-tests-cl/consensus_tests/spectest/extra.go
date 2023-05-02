package spectest

import "io/fs"

type UnimplementedHandler struct {
}

func (u *UnimplementedHandler) Run(root fs.FS) (passed bool, err error) {
	return false, ErrorHandlerNotImplemented
}
