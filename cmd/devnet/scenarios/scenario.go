package scenarios

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"runtime"
	"unicode"
)

var (
	ErrUnmatchedStepArgumentNumber = errors.New("func received more arguments than expected")
	ErrCannotConvert               = errors.New("cannot convert argument")
	ErrUnsupportedArgumentType     = errors.New("unsupported argument type")
)

var stepRunnerRegistry = map[reflect.Value]*stepRunner{}

type stepHandler struct {
	handler          reflect.Value
	matchExpressions []string
}

func RegisterStepHandlers(handlers ...stepHandler) error {
	for _, h := range handlers {
		var exprs []*regexp.Regexp

		if kind := h.handler.Kind(); kind != reflect.Func {
			return fmt.Errorf("Can't register non-function %s as step handler", kind)
		}

		if len(h.matchExpressions) == 0 {
			name := path.Ext(runtime.FuncForPC(h.handler.Pointer()).Name())[1:]

			if unicode.IsLower(rune(name[0])) {
				return fmt.Errorf("Can't register unexported function %s as step handler", name)
			}

			h.matchExpressions = []string{
				name,
			}
		}

		for _, e := range h.matchExpressions {
			exp, err := regexp.Compile(e)

			if err != nil {
				return err
			}

			exprs = append(exprs, exp)
		}

		stepRunnerRegistry[h.handler] = &stepRunner{
			Handler: h.handler,
			Exprs:   exprs,
		}
	}

	return nil
}

func MustRegisterStepHandlers(handlers ...stepHandler) {
	if err := RegisterStepHandlers(handlers...); err != nil {
		panic(fmt.Errorf("Step handler registration failed: %w", err))
	}
}

func StepHandler(handler interface{}, matchExpressions ...string) stepHandler {
	return stepHandler{reflect.ValueOf(handler), matchExpressions}
}

type Scenario struct {
	Id          string  `json:"id"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Steps       []*Step `json:"steps"`
}

type Step struct {
	Id          string        `json:"id"`
	Args        []interface{} `json:"args,omitempty"`
	Text        string        `json:"text"`
	Description string        `json:"description,omitempty"`
}

type stepRunner struct {
	Exprs   []*regexp.Regexp
	Handler reflect.Value
	// multistep related
	Nested    bool
	Undefined []string
}

var typeOfBytes = reflect.TypeOf([]byte(nil))

var typeOfContext = reflect.TypeOf((*context.Context)(nil)).Elem()

func (c *stepRunner) Run(ctx context.Context, args []interface{}) (context.Context, interface{}) {
	var values = make([]reflect.Value, 0, len(args))

	typ := c.Handler.Type()
	numIn := typ.NumIn()
	hasCtxIn := numIn > 0 && typ.In(0).Implements(typeOfContext)

	if hasCtxIn {
		values = append(values, reflect.ValueOf(ctx))
		numIn--
	}

	if len(args) < numIn {
		return ctx, fmt.Errorf("Expected %d arguments, matched %d from step", typ.NumIn(), len(args))
	}

	for _, arg := range args {
		values = append(values, reflect.ValueOf(arg))
	}

	res := c.Handler.Call(values)

	if len(res) == 0 {
		return ctx, nil
	}

	r := res[0].Interface()

	if rctx, ok := r.(context.Context); ok {
		if len(res) == 1 {
			return rctx, nil
		}

		res = res[1:]
		ctx = rctx
	}

	if len(res) == 1 {
		return ctx, res[0].Interface()
	}

	var results = make([]interface{}, 0, len(res))

	for _, value := range res {
		results = append(results, value.Interface())
	}

	return ctx, results
}
