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
