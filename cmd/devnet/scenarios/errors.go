package scenarios

import "fmt"

// ErrUndefined is returned in case if step definition was not found
var ErrUndefined = fmt.Errorf("step is undefined")

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
