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

func CreateHandler(api []rpc.API) *handler.Server {

	var graphqlAPI jsonrpc.GraphQLAPI

	for _, rpc := range api {
		if rpc.Service == nil {
			continue
		}

		if graphqlCandidate, ok := rpc.Service.(jsonrpc.GraphQLAPI); ok {
			graphqlAPI = graphqlCandidate
		}
	}

	resolver := graph.Resolver{}
	resolver.GraphQLAPI = graphqlAPI

	return handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{Resolvers: &resolver})) // TODO : init resolver.DB here !!!
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
