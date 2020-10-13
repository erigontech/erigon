// +build !windows

package mdbx

/*
#include "mdbxgo.h"
#include "dist/mdbx.h"
*/
import "C"
import (
	"syscall"
)

func operrno(op string, ret C.int) error {
	if ret == C.MDBX_SUCCESS || ret == C.MDBX_RESULT_TRUE {
		return nil
	}
	if minErrno <= ret && ret <= maxErrno {
		return &OpError{Op: op, Errno: Errno(ret)}
	}
	return &OpError{Op: op, Errno: syscall.Errno(ret)}
}
