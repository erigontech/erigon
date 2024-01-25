// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpc

import "fmt"

const (
	// Engine API errors
	SERVER_ERROR         = -32000
	INVALID_REQUEST      = -32600
	METHOD_NOT_FOUND     = -32601
	INVALID_PARAMS_ERROR = -32602
	INTERNAL_ERROR       = -32603
	INVALID_JSON         = -32700

	UNKNOWN_PAYLOAD            = -38001
	INVALID_FORKCHOICE_STATE   = -38002
	INVALID_PAYLOAD_ATTRIBUTES = -38003
	TOO_LARGE_REQUEST          = -38004
	UNSUPPORTED_FORK_ERROR     = -38005
)

var (
	_ Error = new(methodNotFoundError)
	_ Error = new(subscriptionNotFoundError)
	_ Error = new(parseError)
	_ Error = new(invalidRequestError)
	_ Error = new(invalidMessageError)
	_ Error = new(InvalidParamsError)
	_ Error = new(CustomError)
)

const defaultErrorCode = -32000

type methodNotFoundError struct{ method string }

func (e *methodNotFoundError) ErrorCode() int { return -32601 }

func (e *methodNotFoundError) Error() string {
	return fmt.Sprintf("the method %s does not exist/is not available", e.method)
}

type subscriptionNotFoundError struct{ namespace, subscription string }

func (e *subscriptionNotFoundError) ErrorCode() int { return -32601 }

func (e *subscriptionNotFoundError) Error() string {
	return fmt.Sprintf("no %q subscription in %s namespace", e.subscription, e.namespace)
}

// Invalid JSON was received by the server.
type parseError struct{ message string }

func (e *parseError) ErrorCode() int { return -32700 }

func (e *parseError) Error() string { return e.message }

// received message isn't a valid request
type invalidRequestError struct{ message string }

func (e *invalidRequestError) ErrorCode() int { return -32600 }

func (e *invalidRequestError) Error() string { return e.message }

// received message is invalid
type invalidMessageError struct{ message string }

func (e *invalidMessageError) ErrorCode() int { return -32700 }

func (e *invalidMessageError) Error() string { return e.message }

// unable to decode supplied params, or invalid parameters
type InvalidParamsError struct{ Message string }

func (e *InvalidParamsError) ErrorCode() int { return -32602 }

func (e *InvalidParamsError) Error() string { return e.Message }

// mismatch between the Engine API method version and the fork
type UnsupportedForkError struct{ Message string }

func (e *UnsupportedForkError) ErrorCode() int { return -38005 }

func (e *UnsupportedForkError) Error() string { return e.Message }

type CustomError struct {
	Code    int
	Message string
}

func (e *CustomError) ErrorCode() int { return e.Code }

func (e *CustomError) Error() string { return e.Message }

func MakeError(errCode int, msg string) *CustomError {
	return &CustomError{Code: errCode, Message: msg}
}
