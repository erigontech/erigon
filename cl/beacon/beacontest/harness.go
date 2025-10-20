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

package beacontest

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"text/template"

	"github.com/Masterminds/sprig/v3"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

type HarnessOption func(*Harness) error

func WithTesting(t *testing.T) func(*Harness) error {
	return func(h *Harness) error {
		h.t = t
		return nil
	}
}

func WithTests(name string, xs []Test) func(*Harness) error {
	return func(h *Harness) error {
		h.tests[name] = xs
		return nil
	}
}

func WithHandler(name string, handler http.Handler) func(*Harness) error {
	return func(h *Harness) error {
		h.handlers[name] = handler
		return nil
	}
}

func WithFilesystem(name string, handler afero.Fs) func(*Harness) error {
	return func(h *Harness) error {
		h.fss[name] = handler
		return nil
	}
}
func WithTestFromFs(fs afero.Fs, name string) func(*Harness) error {
	return func(h *Harness) error {
		filename := name
		for _, fn := range []string{name, name + ".yaml", name + ".yml", name + ".json"} {
			// check if file exists
			_, err := fs.Stat(fn)
			if err == nil {
				filename = fn
				break
			}
		}
		xs, err := afero.ReadFile(fs, filename)
		if err != nil {
			return err
		}
		return WithTestFromBytes(name, xs)(h)
	}
}

type Extra struct {
	Vars map[string]any `json:"vars"`

	RawBodyZZZZ json.RawMessage `json:"tests"`
}

func WithTestFromBytes(name string, xs []byte) func(*Harness) error {
	return func(h *Harness) error {
		var t struct {
			T []Test `json:"tests"`
		}
		x := &Extra{}
		s := md5.New()
		s.Write(xs)
		hsh := hex.EncodeToString(s.Sum(nil))
		// unmarshal just the extra data
		err := yaml.Unmarshal(xs, &x, yaml.JSONOpt(func(d *json.Decoder) *json.Decoder {
			return d
		}))
		if err != nil {
			return err
		}
		tmpl := template.Must(template.New(hsh).Funcs(sprig.FuncMap()).Parse(string(xs)))
		// execute the template using the extra data as the provided top level object
		// we can use the original buffer as the output since the original buffer has already been copied when it was passed into template
		buf := bytes.NewBuffer(xs)
		buf.Reset()
		err = tmpl.Execute(buf, x)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(buf.Bytes(), &t)
		if err != nil {
			return err
		}
		if len(t.T) == 0 {
			return fmt.Errorf("suite with name %s had no tests", name)
		}
		h.tests[name] = t.T
		return nil
	}
}

type Harness struct {
	tests map[string][]Test
	t     *testing.T

	handlers map[string]http.Handler
	fss      map[string]afero.Fs
}

func Execute(options ...HarnessOption) {
	h := &Harness{
		handlers: map[string]http.Handler{},
		tests:    map[string][]Test{},
		fss: map[string]afero.Fs{
			"": afero.NewOsFs(),
		},
	}
	for _, v := range options {
		err := v(h)
		if err != nil {
			h.t.Error(err)
		}
	}
	h.Execute()
}

func (h *Harness) Execute() {
	ctx := context.Background()
	for suiteName, tests := range h.tests {
		for idx, v := range tests {
			v.Actual.h = h
			v.Expect.h = h
			name := v.Name
			if name == "" {
				name = "test"
			}
			fullname := fmt.Sprintf("%s_%s_%d", suiteName, name, idx)
			h.t.Run(fullname, func(t *testing.T) {
				err := v.Execute(ctx, t)
				require.NoError(t, err)
			})
		}
	}
}

type Test struct {
	Name    string     `json:"name"`
	Expect  Source     `json:"expect"`
	Actual  Source     `json:"actual"`
	Compare Comparison `json:"compare"`
}

func (c *Test) Execute(ctx context.Context, t *testing.T) error {
	a, aCode, err := c.Expect.Execute(ctx)
	if err != nil {
		return fmt.Errorf("get expect data: %w", err)
	}
	b, bCode, err := c.Actual.Execute(ctx)
	if err != nil {
		return fmt.Errorf("get actual data: %w", err)
	}
	err = c.Compare.Compare(t, a, b, aCode, bCode)
	if err != nil {
		return fmt.Errorf("compare: %w", err)
	}

	return nil
}

type Comparison struct {
	Expr    string   `json:"expr"`
	Exprs   []string `json:"exprs"`
	Literal bool     `json:"literal"`
}

func (c *Comparison) Compare(t *testing.T, aRaw, bRaw json.RawMessage, aCode, bCode int) error {
	var err error
	var a, b any
	var aType, bType *types.Type

	if !c.Literal {
		var aMap, bMap any
		err = yaml.Unmarshal(aRaw, &aMap)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(bRaw, &bMap)
		if err != nil {
			return err
		}
		a = aMap
		b = bMap
		if a != nil {
			switch reflect.TypeOf(a).Kind() {
			case reflect.Slice:
				aType = cel.ListType(cel.MapType(cel.StringType, cel.DynType))
			default:
				aType = cel.MapType(cel.StringType, cel.DynType)
			}
		} else {
			aType = cel.MapType(cel.StringType, cel.DynType)
		}
		if b != nil {
			switch reflect.TypeOf(b).Kind() {
			case reflect.Slice:
				bType = cel.ListType(cel.MapType(cel.StringType, cel.DynType))
			default:
				bType = cel.MapType(cel.StringType, cel.DynType)
			}
		} else {
			bType = cel.MapType(cel.StringType, cel.DynType)
		}
	} else {
		a = string(aRaw)
		b = string(bRaw)
		aType = cel.StringType
		bType = cel.StringType
	}

	exprs := []string{}
	// if no default expr set and no exprs are set, then add the default expr
	if len(c.Exprs) == 0 && c.Expr == "" {
		exprs = append(exprs, "actual_code == 200", "actual == expect")
	}

	env, err := cel.NewEnv(
		cel.Variable("expect", aType),
		cel.Variable("actual", bType),
		cel.Variable("expect_code", cel.IntType),
		cel.Variable("actual_code", cel.IntType),
	)
	if err != nil {
		return err
	}

	for _, expr := range append(c.Exprs, exprs...) {
		ast, issues := env.Compile(expr)
		if issues != nil && issues.Err() != nil {
			return issues.Err()
		}
		prg, err := env.Program(ast)
		if err != nil {
			return fmt.Errorf("program construction error: %w", err)
		}
		res, _, err := prg.Eval(map[string]any{
			"expect":      a,
			"actual":      b,
			"expect_code": aCode,
			"actual_code": bCode,
		})
		if err != nil {
			return err
		}
		if res.Type() != cel.BoolType {
			return ErrExpressionMustReturnBool
		}
		bres, ok := res.Value().(bool)
		if !ok {
			return ErrExpressionMustReturnBool
		}
		if !assert.True(t, bres, `expr: %s`, expr) {
			if os.Getenv("HIDE_HARNESS_LOG") != "1" {
				// b1, _ := json.Marshal(b)
				// panic(string(b1))
				t.Logf(`name: %s
				expect%d: %v
				actual%d: %v
				expr: %s
				`, t.Name(), aCode, a, bCode, b, expr)

			}
			t.FailNow()
		}
	}
	return nil
}

type Source struct {
	// backref to the harness
	h *Harness `json:"-"`

	// remote type
	Remote  *string           `json:"remote,omitempty"`
	Handler *string           `json:"handler,omitempty"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Query   map[string]string `json:"query"`
	Headers map[string]string `json:"headers"`
	Body    *Source           `json:"body,omitempty"`

	// data type
	Data any `json:"data,omitempty"`

	// file type
	File *string `json:"file,omitempty"`
	Fs   string  `json:"fs,omitempty"`

	// for raw type
	Raw *string `json:"raw,omitempty"`
}

func (s *Source) Execute(ctx context.Context) (json.RawMessage, int, error) {
	if s.Raw != nil {
		return s.executeRaw(ctx)
	}
	if s.File != nil {
		return s.executeFile(ctx)
	}
	if s.Remote != nil || s.Handler != nil {
		return s.executeRemote(ctx)
	}
	if s.Data != nil {
		return s.executeData(ctx)
	}
	return s.executeEmpty(ctx)
}
func (s *Source) executeRemote(ctx context.Context) (json.RawMessage, int, error) {
	method := "GET"
	if s.Method != "" {
		method = s.Method
	}
	method = strings.ToUpper(method)
	var body io.Reader
	// hydrate the harness
	if s.Body != nil {
		s.Body.h = s.h
		msg, _, err := s.Body.Execute(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("getting body: %w", err)
		}
		body = bytes.NewBuffer(msg)
	}
	var purl *url.URL
	if s.Remote != nil {
		niceUrl, err := url.Parse(*s.Remote)
		if err != nil {
			return nil, 0, err
		}
		purl = niceUrl

	} else if s.Handler != nil {
		handler, ok := s.h.handlers[*s.Handler]
		if !ok {
			return nil, 0, fmt.Errorf("handler not registered: %s", *s.Handler)
		}
		server := httptest.NewServer(handler)
		defer server.Close()
		niceUrl, err := url.Parse(server.URL)
		if err != nil {
			return nil, 0, err
		}
		purl = niceUrl
	} else {
		panic("impossible code path. bug? source.Execute() should ensure this never happens")
	}

	purl = purl.JoinPath(s.Path)
	q := purl.Query()
	for k, v := range s.Query {
		q.Add(k, v)
	}
	purl.RawQuery = q.Encode()

	request, err := http.NewRequest(method, strings.ReplaceAll(purl.String(), "%3F", "?"), body)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range s.Headers {
		request.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, resp.StatusCode, nil
	}
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, 200, err
	}
	return json.RawMessage(out), 200, nil
}

func (s *Source) executeData(ctx context.Context) (json.RawMessage, int, error) {
	ans, err := json.Marshal(s.Data)
	if err != nil {
		return nil, 400, nil
	}
	return ans, 200, nil
}

func (s *Source) executeFile(ctx context.Context) (json.RawMessage, int, error) {
	afs, ok := s.h.fss[s.Fs]
	if !ok {
		return nil, 404, fmt.Errorf("filesystem %s not defined", s.Fs)
	}
	name := *s.File
	filename := name
	for _, fn := range []string{name, name + ".yaml", name + ".yml", name + ".json"} {
		// check if file exists
		_, err := afs.Stat(fn)
		if err == nil {
			filename = fn
			break
		}
	}
	fileBytes, err := afero.ReadFile(afs, filename)
	if err != nil {
		return nil, 404, err
	}
	return json.RawMessage(fileBytes), 200, nil
}
func (s *Source) executeRaw(ctx context.Context) (json.RawMessage, int, error) {
	return json.RawMessage(*s.Raw), 200, nil
}

func (s *Source) executeEmpty(ctx context.Context) (json.RawMessage, int, error) {
	return []byte("{}"), 200, nil
}
