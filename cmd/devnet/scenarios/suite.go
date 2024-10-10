package scenarios

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/log/v3"
)

type SimulationContext struct {
	suite *suite
}

type BeforeScenarioHook func(context.Context, *Scenario) (context.Context, error)
type AfterScenarioHook func(context.Context, *Scenario, error) (context.Context, error)
type BeforeStepHook func(context.Context, *Step) (context.Context, error)
type AfterStepHook func(context.Context, *Step, StepStatus, error) (context.Context, error)

var TimeNowFunc = func() time.Time {
	return time.Now()
}

type suite struct {
	stepRunners    []*stepRunner
	failed         bool
	randomize      bool
	strict         bool
	stopOnFailure  bool
	testingT       *testing.T
	defaultContext context.Context

	// suite event handlers
	beforeScenarioHandlers []BeforeScenarioHook
	afterScenarioHandlers  []AfterScenarioHook
	beforeStepHandlers     []BeforeStepHook
	afterStepHandlers      []AfterStepHook
}

func (s *suite) runSteps(ctx context.Context, scenario *Scenario, steps []*Step) (context.Context, []StepResult, error) {
	var results = make([]StepResult, 0, len(steps))
	var err error

	logger := devnet.Logger(ctx)

	for i, step := range steps {
		isLast := i == len(steps)-1
		isFirst := i == 0

		var stepResult StepResult

		ctx, stepResult = s.runStep(ctx, scenario, step, err, isFirst, isLast, logger)

		switch {
		case stepResult.Err == nil:
		case errors.Is(stepResult.Err, ErrUndefined):
			// do not overwrite failed error
			if err == nil {
				err = stepResult.Err
			}
		default:
			err = stepResult.Err
			logger.Error("Step failed with error", "scenario", scenario.Name, "step", step.Text, "err", err)
		}

		results = append(results, stepResult)
	}

	return ctx, results, err
}

func (s *suite) runStep(ctx context.Context, scenario *Scenario, step *Step, prevStepErr error, isFirst, isLast bool, logger log.Logger) (rctx context.Context, sr StepResult) {
	var match *stepRunner

	sr = StepResult{Status: Undefined}
	rctx = ctx

	// user multistep definitions may panic
	defer func() {
		if e := recover(); e != nil {
			logger.Error("Step failed with panic", "scenario", scenario.Name, "step", step.Text, "err", e)
			sr.Err = &traceError{
				msg:   fmt.Sprintf("%v", e),
				stack: callStack(),
			}
		}

		earlyReturn := prevStepErr != nil || sr.Err == ErrUndefined

		// Run after step handlers.
		rctx, sr.Err = s.runAfterStepHooks(ctx, step, sr.Status, sr.Err)

		// Trigger after scenario on failing or last step to attach possible hook error to step.
		if isLast || (sr.Status != Skipped && sr.Status != Undefined && sr.Err != nil) {
			rctx, sr.Err = s.runAfterScenarioHooks(rctx, scenario, sr.Err)
		}

		if earlyReturn {
			return
		}

		switch sr.Err {
		case nil:
			sr.Status = Passed
		default:
			sr.Status = Failed
		}
	}()

	// run before scenario handlers
	if isFirst {
		ctx, sr.Err = s.runBeforeScenarioHooks(ctx, scenario)
	}

	// run before step handlers
	ctx, sr.Err = s.runBeforeStepHooks(ctx, step, sr.Err)

	if sr.Err != nil {
		sr = NewStepResult(step.Id, step)
		sr.Status = Failed
		return ctx, sr
	}

	ctx, undef, match, err := s.maybeUndefined(ctx, step.Text, step.Args, logger)

	if err != nil {
		return ctx, sr
	} else if len(undef) > 0 {
		sr = NewStepResult(scenario.Id, step)
		sr.Status = Undefined
		sr.Err = ErrUndefined
		logger.Error("Step failed undefined step", "scenario", scenario.Name, "step", step.Text)
		return ctx, sr
	}

	if prevStepErr != nil {
		sr = NewStepResult(scenario.Id, step)
		sr.Status = Skipped
		return ctx, sr
	}

	ctx, res := match.Run(ctx, step.Text, step.Args, logger)
	ctx, sr.Returns, sr.Err = s.maybeSubSteps(ctx, res, logger)

	return ctx, sr
}

func (s *suite) maybeUndefined(ctx context.Context, text string, args []interface{}, logger log.Logger) (context.Context, []string, *stepRunner, error) {
	step := s.matchStep(text)

	if nil == step {
		return ctx, []string{text}, nil, nil
	}

	var undefined []string

	if !step.Nested {
		return ctx, undefined, step, nil
	}

	ctx, steps := step.Run(ctx, text, args, logger)

	for _, next := range steps.([]Step) {
		ctx, undef, _, err := s.maybeUndefined(ctx, next.Text, nil, logger)
		if err != nil {
			return ctx, undefined, nil, err
		}
		undefined = append(undefined, undef...)
	}

	return ctx, undefined, nil, nil
}

func (s *suite) matchStep(text string) *stepRunner {
	var matches []*stepRunner

	for _, r := range s.stepRunners {
		for _, expr := range r.Exprs {
			if m := expr.FindStringSubmatch(text); len(m) > 0 {
				matches = append(matches, r)
			}
		}
	}

	if len(matches) == 1 {
		return matches[0]
	}

	for _, r := range matches {
		for _, expr := range r.Exprs {
			if m := expr.FindStringSubmatch(text); m[0] == text {
				return r
			}
		}
	}

	for _, r := range stepRunnerRegistry {
		for _, expr := range r.Exprs {
			if m := expr.FindStringSubmatch(text); len(m) > 0 {
				matches = append(matches, r)
			}
		}
	}

	if len(matches) == 1 {
		return matches[0]
	}

	for _, r := range matches {
		for _, expr := range r.Exprs {
			if m := expr.FindStringSubmatch(text); m[0] == text {
				return r
			}
		}
	}

	return nil
}

func (s *suite) maybeSubSteps(ctx context.Context, result interface{}, logger log.Logger) (context.Context, []interface{}, error) {
	if nil == result {
		return ctx, nil, nil
	}

	var steps []Step

	switch result := result.(type) {
	case error:
		return ctx, nil, result
	case []Step:
		steps = result
	case []interface{}:
		if len(result) == 0 {
			return ctx, result, nil
		}

		if err, ok := result[len(result)-1].(error); ok {
			return ctx, result[0 : len(result)-1], err
		}

		return ctx, result, nil
	default:
		return ctx, nil, fmt.Errorf("unexpected results type: %T - %+v", result, result)
	}

	var err error

	for _, step := range steps {
		if def := s.matchStep(step.Text); def == nil {
			return ctx, nil, ErrUndefined
		} else {
			ctx, res := def.Run(ctx, step.Text, step.Args, logger)
			if ctx, _, err = s.maybeSubSteps(ctx, res, logger); err != nil {
				return ctx, nil, fmt.Errorf("%s: %+v", step.Text, err)
			}
		}
	}
	return ctx, nil, nil
}

func (s *suite) runScenario(scenario *Scenario) (sr *ScenarioResult, err error) {
	ctx := s.defaultContext

	if scenario.Context != nil {
		ctx = JoinContexts(scenario.Context, ctx)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	if len(scenario.Steps) == 0 {
		return &ScenarioResult{ScenarioId: scenario.Id, StartedAt: TimeNowFunc()}, ErrUndefined
	}

	// Before scenario hooks are called in context of first evaluated step
	// so that error from handler can be added to step.

	sr = &ScenarioResult{ScenarioId: scenario.Id, StartedAt: TimeNowFunc()}

	// scenario
	if s.testingT != nil {
		// Running scenario as a subtest.
		s.testingT.Run(scenario.Name, func(t *testing.T) {
			ctx, sr.StepResults, err = s.runSteps(ctx, scenario, scenario.Steps)
			if s.shouldFail(err) {
				t.Error(err)
			}
		})
	} else {
		ctx, sr.StepResults, err = s.runSteps(ctx, scenario, scenario.Steps)
	}

	// After scenario handlers are called in context of last evaluated step
	// so that error from handler can be added to step.

	return sr, err
}

func (s *suite) shouldFail(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, ErrUndefined) {
		return s.strict
	}

	return true
}

func (s *suite) runBeforeStepHooks(ctx context.Context, step *Step, err error) (context.Context, error) {
	hooksFailed := false

	for _, f := range s.beforeStepHandlers {
		hctx, herr := f(ctx, step)
		if herr != nil {
			hooksFailed = true

			if err == nil {
				err = herr
			} else {
				err = fmt.Errorf("%v, %w", herr, err)
			}
		}

		if hctx != nil {
			ctx = hctx
		}
	}

	if hooksFailed {
		err = fmt.Errorf("before step hook failed: %w", err)
	}

	return ctx, err
}

func (s *suite) runAfterStepHooks(ctx context.Context, step *Step, status StepStatus, err error) (context.Context, error) {
	for _, f := range s.afterStepHandlers {
		hctx, herr := f(ctx, step, status, err)

		// Adding hook error to resulting error without breaking hooks loop.
		if herr != nil {
			if err == nil {
				err = herr
			} else {
				err = fmt.Errorf("%v, %w", herr, err)
			}
		}

		if hctx != nil {
			ctx = hctx
		}
	}

	return ctx, err
}

func (s *suite) runBeforeScenarioHooks(ctx context.Context, scenario *Scenario) (context.Context, error) {
	var err error

	// run before scenario handlers
	for _, f := range s.beforeScenarioHandlers {
		hctx, herr := f(ctx, scenario)
		if herr != nil {
			if err == nil {
				err = herr
			} else {
				err = fmt.Errorf("%v, %w", herr, err)
			}
		}

		if hctx != nil {
			ctx = hctx
		}
	}

	if err != nil {
		err = fmt.Errorf("before scenario hook failed: %w", err)
	}

	return ctx, err
}

func (s *suite) runAfterScenarioHooks(ctx context.Context, scenario *Scenario, lastStepErr error) (context.Context, error) {
	err := lastStepErr

	hooksFailed := false
	isStepErr := true

	// run after scenario handlers
	for _, f := range s.afterScenarioHandlers {
		hctx, herr := f(ctx, scenario, err)

		// Adding hook error to resulting error without breaking hooks loop.
		if herr != nil {
			hooksFailed = true

			if err == nil {
				isStepErr = false
				err = herr
			} else {
				if isStepErr {
					err = fmt.Errorf("step error: %w", err)
					isStepErr = false
				}
				err = fmt.Errorf("%v, %w", herr, err)
			}
		}

		if hctx != nil {
			ctx = hctx
		}
	}

	if hooksFailed {
		err = fmt.Errorf("after scenario hook failed: %w", err)
	}

	return ctx, err
}
