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

package errors

import (
	"context"
	"errors"
)

// NilIfCanceled returns nil when err is nil or contains only context
// cancellation. Mixed errors are preserved.
func NilIfCanceled(err error) error {
	if err == nil || IsOnlyCanceled(err) {
		return nil
	}
	return err
}

// IsOnlyCanceled reports whether err is non-nil and every leaf in its unwrap
// tree matches context.Canceled. It deliberately ignores custom Is methods on
// non-leaf errors so they cannot hide a non-cancellation cause.
func IsOnlyCanceled(err error) bool {
	if err == nil {
		return false
	}
	switch x := err.(type) {
	case interface{ Unwrap() []error }:
		errs := x.Unwrap()
		if len(errs) == 0 {
			return false
		}
		for _, e := range errs {
			if !IsOnlyCanceled(e) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return IsOnlyCanceled(x.Unwrap())
	default:
		return errors.Is(err, context.Canceled)
	}
}

func IsOneOf(err error, targets []error) bool {
	if err == nil {
		return false
	}

	for _, target := range targets {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
}
