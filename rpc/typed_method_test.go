// Copyright 2026 The Erigon Authors
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

package rpc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

type typedTestService struct{}

type typedTestArgs struct {
	S string `json:"s"`
}

type typedTestResult struct {
	S         string
	B         *int
	C         *string
	D         *[]int
	Transport string
}

func (typedTestService) Call(ctx context.Context, a typedTestArgs, b *int, c *string, d *[]int) (typedTestResult, error) {
	switch a.S {
	case "panic":
		panic("boom")
	case "error":
		return typedTestResult{}, testError{}
	}
	return typedTestResult{a.S, b, c, d, PeerInfoFromContext(ctx).Transport}, nil
}

func (typedTestService) Other(s string) string { return s }

func typedTestAPI() API {
	return API{Namespace: "t", Service: typedTestService{}, Typed: map[string]TypedMethod{"call": Method4(typedTestService{}.Call)}}
}

// newTypedTestServers returns a server that calls t_call by reflection and one that calls it typed.
func newTypedTestServers(t testing.TB) (reflected, typed *Server) {
	reflected = NewServer(50, false, false, true, log.New(), 100)
	typed = NewServer(50, false, false, true, log.New(), 100)
	t.Cleanup(reflected.Stop)
	t.Cleanup(typed.Stop)
	require.NoError(t, reflected.RegisterName("t", typedTestService{}))
	require.NoError(t, typed.RegisterAPI(typedTestAPI()))
	require.NotNil(t, typed.services.callback("t_call").typed)
	require.Nil(t, typed.services.callback("t_other").typed)
	return reflected, typed
}

func serveTestBody(t testing.TB, srv *Server, body string) (int, string) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("content-type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func requireSameResponse(t testing.TB, reflected, typed *Server, body string) {
	wantCode, want := serveTestBody(t, reflected, body)
	gotCode, got := serveTestBody(t, typed, body)
	require.Equal(t, wantCode, gotCode, body)
	require.Equal(t, want, got, body)
}

func TestTypedMethodMatchesReflection(t *testing.T) {
	reflected, typed := newTypedTestServers(t)
	for _, params := range []string{
		``,
		`null`,
		`[]`,
		`[null]`,
		`[{}]`,
		`[{"s":"x"}]`,
		`[{"s":"x"},1]`,
		`[{"s":"x"},null,"c"]`,
		`[{"s":"x"},1,"c",[1,2]]`,
		`[{"s":"x"},1,"c",[1,2],5]`,
		`[{"s":"x"},"1"]`,
		`[{"s":1}]`,
		`["x"]`,
		`{"s":"x"}`,
		`[{"s":"panic"}]`,
		`[{"s":"error"}]`,
	} {
		body := `{"jsonrpc":"2.0","id":1,"method":"t_call"`
		if params != "" {
			body += `,"params":` + params
		}
		body += `}`
		requireSameResponse(t, reflected, typed, body)
	}
	requireSameResponse(t, reflected, typed, `{"jsonrpc":"2.0","method":"t_call","params":[{"s":"x"}]}`)
	requireSameResponse(t, reflected, typed, `[{"jsonrpc":"2.0","id":1,"method":"t_call","params":[{"s":"x"}]},{"jsonrpc":"2.0","id":2,"method":"t_call","params":[{"s":"error"}]}]`)
}

func TestTypedMethodRegistration(t *testing.T) {
	srv := NewServer(50, false, false, true, log.New(), 100)
	defer srv.Stop()
	api := typedTestAPI()
	api.Typed = map[string]TypedMethod{"missing": Method4(typedTestService{}.Call)}
	require.ErrorContains(t, srv.RegisterAPI(api), "missing")
	api.Typed = map[string]TypedMethod{"other": Method4(typedTestService{}.Call)}
	require.ErrorContains(t, srv.RegisterAPI(api), "other")
}

func FuzzTypedMethodMatchesReflection(f *testing.F) {
	for _, params := range []string{`[{"s":"x"},1,"c",[1]]`, `[null]`, `[{"s":"error"}]`, `{}`, `[1,2,3,4,5]`} {
		f.Add(params)
	}
	reflected, typed := newTypedTestServers(f)
	f.Fuzz(func(t *testing.T, params string) {
		requireSameResponse(t, reflected, typed, `{"jsonrpc":"2.0","id":1,"method":"t_call","params":`+params+`}`)
	})
}
