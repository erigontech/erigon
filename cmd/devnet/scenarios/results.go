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
	"time"
)

type ScenarioResult struct {
	ScenarioId string
	StartedAt  time.Time

	StepResults []StepResult
}

type StepResult struct {
	Status     StepStatus
	FinishedAt time.Time
	Err        error
	Returns    []interface{}
	ScenarioId string

	Step *Step
}

func NewStepResult(scenarioId string, step *Step) StepResult {
	return StepResult{FinishedAt: TimeNowFunc(), ScenarioId: scenarioId, Step: step}
}

type StepStatus int

const (
	Passed StepStatus = iota
	Failed
	Skipped
	Undefined
	Pending
)

// String ...
func (st StepStatus) String() string {
	switch st {
	case Passed:
		return "passed"
	case Failed:
		return "failed"
	case Skipped:
		return "skipped"
	case Undefined:
		return "undefined"
	case Pending:
		return "pending"
	default:
		return "unknown"
	}
}
