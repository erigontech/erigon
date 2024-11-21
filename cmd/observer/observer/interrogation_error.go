// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package observer

import "fmt"

type InterrogationErrorID int

const (
	InterrogationErrorPing InterrogationErrorID = iota + 1
	InterrogationErrorENRDecode
	InterrogationErrorIncompatibleForkID
	InterrogationErrorBlacklistedClientID
	InterrogationErrorKeygen
	InterrogationErrorFindNode
	InterrogationErrorFindNodeTimeout
	InterrogationErrorCtxCancelled
)

type InterrogationError struct {
	id         InterrogationErrorID
	wrappedErr error
}

func NewInterrogationError(id InterrogationErrorID, wrappedErr error) *InterrogationError {
	instance := InterrogationError{
		id,
		wrappedErr,
	}
	return &instance
}

func (e *InterrogationError) Unwrap() error {
	return e.wrappedErr
}

func (e *InterrogationError) Error() string {
	switch e.id {
	case InterrogationErrorPing:
		return fmt.Sprintf("ping-pong failed: %v", e.wrappedErr)
	case InterrogationErrorENRDecode:
		return e.wrappedErr.Error()
	case InterrogationErrorIncompatibleForkID:
		return fmt.Sprintf("incompatible ENR fork ID %v", e.wrappedErr)
	case InterrogationErrorBlacklistedClientID:
		return fmt.Sprintf("incompatible client ID %v", e.wrappedErr)
	case InterrogationErrorKeygen:
		return fmt.Sprintf("keygen failed: %v", e.wrappedErr)
	case InterrogationErrorFindNode:
		return fmt.Sprintf("FindNode request failed: %v", e.wrappedErr)
	case InterrogationErrorFindNodeTimeout:
		return fmt.Sprintf("FindNode request timeout: %v", e.wrappedErr)
	default:
		return "<unhandled InterrogationErrorID>"
	}
}
