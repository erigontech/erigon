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

package scenarios

import (
	"errors"
	"fmt"
)

// ErrUndefined is returned in case if step definition was not found
var ErrUndefined = errors.New("step is undefined")

type ScenarioError struct {
	error
	Result ScenarioResult
	Cause  error
}

func (e *ScenarioError) Unwrap() error {
	return e.error
}

func NewScenarioError(err error, result ScenarioResult, cause error) *ScenarioError {
	return &ScenarioError{err, result, cause}
}

func (e *ScenarioError) Error() string {
	return fmt.Sprintf("%s: Cause: %s", e.error, e.Cause)
}
