package mdbx

/*
#include <errno.h>
#include "mdbxgo.h"
#include "config.h"
#include "mdbx.h"
*/
import "C"
import "syscall"

func operrno(op string, ret C.int) error {
	if ret == C.MDBX_SUCCESS || ret == C.MDBX_RESULT_TRUE {
		return nil
	}
	if minErrno <= ret && ret <= maxErrno {
		return &OpError{Op: op, Errno: Errno(ret)}
	}

	// translate C errors into corresponding syscall.Errno values so that
	// IsErrnoSys functions correctly, a kludge unknowning inherited from LMDB.
	// the errno in the returned OpError cannot be passed to C.mdbx_strerror.
	// see the implementation of C.mdbx_strerror for information about how the
	// following table was generated.
	var errno syscall.Errno
	switch ret {
	case C.ENOENT:
		errno = syscall.ENOENT /* 2, FILE_NOT_FOUND */
	case C.EIO:
		errno = syscall.EIO /* 5, ACCESS_DENIED */
	case C.ENOMEM:
		errno = syscall.ENOMEM /* 12, INVALID_ACCESS */
	case C.EACCES:
		errno = syscall.EACCES /* 13, INVALID_DATA */
	case C.EBUSY:
		errno = syscall.EBUSY /* 16, CURRENT_DIRECTORY */
	case C.EINVAL:
		errno = syscall.EINVAL /* 22, BAD_COMMAND */
	case C.ENOSPC:
		errno = syscall.ENOSPC /* 28, OUT_OF_PAPER */
	default:
		errno = syscall.Errno(ret)
	}
	return &OpError{Op: op, Errno: errno}
}
