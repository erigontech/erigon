// Copyright 2025 The Erigon Authors
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

package integrity

import (
	"errors"
	"fmt"
)

// ErrIntegrity is useful to differentiate integrity errors from program errors.
var ErrIntegrity = errors.New("integrity error")

// AccumulateIntegrityError is useful when we want to accumulate multiple integrity errors.
func AccumulateIntegrityError(baseErr, newErr error) error {
	if !errors.Is(baseErr, ErrIntegrity) {
		baseErr = fmt.Errorf("%w: %w", ErrIntegrity, baseErr)
	}
	return fmt.Errorf("%w: %w", baseErr, newErr)
}
