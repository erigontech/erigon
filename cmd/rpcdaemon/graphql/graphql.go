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
	"encoding/json"
	"net/http"
	"strings"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

const (
	urlPath = "/graphql"
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
	return statusFixMiddleware(srv)
}

// statusFixMiddleware adjusts HTTP status codes to match the GraphQL test expectations:
// - 422 (gqlgen validation errors) → 400
// - 200 with top-level "errors" → 400
func statusFixMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		status := rec.status
		body := rec.buf.Bytes()

		if status == http.StatusUnprocessableEntity {
			status = http.StatusBadRequest
		} else if status == http.StatusOK && hasGraphQLErrors(body) {
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
