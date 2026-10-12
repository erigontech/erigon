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

package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"

	gqlgen "github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

const (
	urlPath = "/graphql"

	// maxQueryDepth bounds field nesting, as geth does, so a chain of block.parent
	// selections cannot fan out into an unbounded number of block reads.
	maxQueryDepth = 20

	maxRequestBodySize = 32 * 1024 * 1024
)

func CreateHandler(api []rpc.API) http.Handler {
	var graphqlAPI jsonrpc.GraphQLAPI

	for _, r := range api {
		if r.Service == nil {
			continue
		}

		if graphqlCandidate, ok := r.Service.(jsonrpc.GraphQLAPI); ok {
			graphqlAPI = graphqlCandidate
		}
	}

	resolver := graph.Resolver{}
	resolver.GraphQLAPI = graphqlAPI

	srv := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &resolver}))
	srv.Use(depthLimit(maxQueryDepth))
	srv.SetErrorPresenter(func(ctx context.Context, err error) *gqlerror.Error {
		if errors.Is(err, kv.ErrReadTxLimitExceeded) {
			if overloaded, _ := ctx.Value(overloadedKey{}).(*atomic.Bool); overloaded != nil {
				overloaded.Store(true)
			}
		}
		return gqlgen.DefaultErrorPresenter(ctx, err)
	})
	return bodyLimitMiddleware(statusFixMiddleware(srv))
}

func bodyLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxRequestBodySize {
			http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
		next.ServeHTTP(w, r)
	})
}

type depthLimit int

var _ gqlgen.OperationContextMutator = depthLimit(0)

func (depthLimit) ExtensionName() string { return "DepthLimit" }

func (depthLimit) Validate(gqlgen.ExecutableSchema) error { return nil }

func (d depthLimit) MutateOperationContext(_ context.Context, rc *gqlgen.OperationContext) *gqlerror.Error {
	if rc.Operation == nil {
		return nil
	}
	if depth := selectionDepth(rc.Operation.SelectionSet, map[string]int{}); depth > int(d) {
		return gqlerror.Errorf("query depth %d exceeds the limit of %d", depth, int(d))
	}
	return nil
}

func selectionDepth(set ast.SelectionSet, fragments map[string]int) int {
	depth := 0
	for _, sel := range set {
		var d int
		switch sel := sel.(type) {
		case *ast.Field:
			d = 1 + selectionDepth(sel.SelectionSet, fragments)
		case *ast.InlineFragment:
			d = selectionDepth(sel.SelectionSet, fragments)
		case *ast.FragmentSpread:
			cached, ok := fragments[sel.Name]
			if !ok && sel.Definition != nil {
				cached = selectionDepth(sel.Definition.SelectionSet, fragments)
				fragments[sel.Name] = cached
			}
			d = cached
		}
		depth = max(depth, d)
	}
	return depth
}

type overloadedKey struct{}

// statusFixMiddleware adjusts HTTP status codes to match the GraphQL test expectations:
// - a resolver hit kv.ErrReadTxLimitExceeded → 503 with Retry-After
// - 422 (gqlgen validation errors) → 400
// - 200 with top-level "errors" → 400
func statusFixMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var overloaded atomic.Bool
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), overloadedKey{}, &overloaded)))

		status := rec.status
		body := rec.buf.Bytes()

		switch {
		case overloaded.Load():
			status = http.StatusServiceUnavailable
			w.Header().Set("Retry-After", "1")
		case status == http.StatusUnprocessableEntity:
			status = http.StatusBadRequest
		case status == http.StatusOK && hasGraphQLErrors(body):
			status = http.StatusBadRequest
		}

		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	return r.buf.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func hasGraphQLErrors(body []byte) bool {
	var result struct {
		Errors json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false
	}
	s := string(result.Errors)
	return len(result.Errors) > 0 && s != "null" && s != "[]"
}

func ProcessGraphQLcheckIfNeeded(
	graphQLHandler http.Handler,
	w http.ResponseWriter,
	r *http.Request,
) bool {
	if strings.EqualFold(r.URL.Path, urlPath) {
		graphQLHandler.ServeHTTP(w, r)
		return true
	}

	if strings.EqualFold(r.URL.Path, urlPath+"/ui") {
		playground.Handler("GraphQL playground", "/graphql").ServeHTTP(w, r)
		return true
	}

	return false
}
