package beacontest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

type Harness struct {
	Tests []Test `json:"tests"`
}

func ExecuteBytes(t *testing.T, xs []byte) {
	var h Harness
	err := yaml.Unmarshal(xs, &h)
	require.NoError(t, err)
	h.Execute(t)
	return
}

func (h *Harness) Execute(t *testing.T) {
	ctx := context.Background()

	for idx, v := range h.Tests {
		name := v.Name
		if name == "" {
			name = fmt.Sprintf("test_%d", idx)
		}
		t.Run(name, func(t *testing.T) {
			passes, err := v.Execute(ctx)
			require.NoError(t, err)
			require.True(t, passes, "compare function returned false")
		})
	}
}

type Test struct {
	Name    string     `json:"name"`
	Expect  Source     `json:"expect"`
	Actual  Source     `json:"actual"`
	Compare Comparison `json:"compare"`
}

func (c *Test) Execute(ctx context.Context) (bool, error) {
	a, err := c.Expect.Execute(ctx)
	if err != nil {
		return false, fmt.Errorf("get expect data: %w", err)
	}
	b, err := c.Actual.Execute(ctx)
	if err != nil {
		return false, fmt.Errorf("get actual data: %w", err)
	}
	ok, err := c.Compare.Compare(a, b)
	if err != nil {
		return false, fmt.Errorf("compare: %w", err)
	}

	return ok, nil
}

type Comparison struct {
	Negate  bool   `json:"neg"`
	Expr    string `json:"expr"`
	Literal bool   `json:"literal"`
}

func (c *Comparison) Compare(aRaw json.RawMessage, bRaw json.RawMessage) (bool, error) {
	var err error
	var a, b any
	var aType, bType *types.Type

	if !c.Literal {
		var aMap, bMap map[string]any
		err = json.Unmarshal(aRaw, &aMap)
		if err != nil {
			return false, err
		}
		err = json.Unmarshal(bRaw, &bMap)
		if err != nil {
			return false, err
		}
		a = aMap
		b = bMap
		aType = cel.MapType(cel.StringType, cel.DynType)
		bType = cel.MapType(cel.StringType, cel.DynType)
	} else {
		a = string(aRaw)
		b = string(bRaw)
		aType = cel.StringType
		bType = cel.StringType
	}

	expr := c.Expr
	if expr == "" {
		expr = "actual == expect"
	}
	env, err := cel.NewEnv(
		cel.Variable("expect", aType),
		cel.Variable("actual", bType),
	)
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return false, issues.Err()
	}
	prg, err := env.Program(ast)
	if err != nil {
		return false, fmt.Errorf("program construction error: %w", err)
	}
	res, _, err := prg.Eval(map[string]any{
		"expect": a,
		"actual": b,
	})
	if err != nil {
		return false, err
	}
	if res.Type() != cel.BoolType {
		return false, ErrExpressionMustReturnBool
	}
	bres, ok := res.Value().(bool)
	if !ok {
		return false, ErrExpressionMustReturnBool
	}
	return bres == !c.Negate, nil
}

type Source struct {
	// remote type
	AbsoluteRemote *string           `json:"absolute_remote,omitempty"`
	Remote         *string           `json:"remote,omitempty"`
	Path           string            `json:"path"`
	Query          map[string]any    `json:"query"`
	Headers        map[string]string `json:"headers"`

	// data type
	Data map[string]any `json:"data,omitempty"`

	// for raw type
	Raw *string `json:"raw,omitempty"`
}

func (s *Source) Execute(ctx context.Context) (json.RawMessage, error) {
	if s.Raw != nil {
		return s.executeRaw(ctx)
	}
	if s.Remote != nil {
		return s.executeRemote(ctx)
	}
	if s.AbsoluteRemote != nil {
		return s.executeRemote(ctx)
	}
	if s.Data != nil {
		return s.executeData(ctx)
	}

	return s.executeUnknown(ctx)
}
func (s *Source) executeRemote(ctx context.Context) (json.RawMessage, error) {
	return json.Marshal(s.Data)
}

func (s *Source) executeData(ctx context.Context) (json.RawMessage, error) {
	return json.Marshal(s.Data)
}

func (s *Source) executeRaw(ctx context.Context) (json.RawMessage, error) {
	return json.RawMessage(*s.Raw), nil
}

func (s *Source) executeUnknown(ctx context.Context) (json.RawMessage, error) {
	return nil, fmt.Errorf("%w: source definition", ErrUnknownType)
}
