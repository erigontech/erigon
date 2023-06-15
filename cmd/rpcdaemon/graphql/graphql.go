package graphql

import (
	"net/http"
	"strings"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/playground"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/graphql/graph"
	"github.com/ledgerwatch/erigon/rpc"
)

const (
	urlPath = "/graphql"
)

func CreateHandler(api []rpc.API) *handler.Server {

	var graphqlAPI commands.GraphQLAPI

	for _, rpc := range api {
		if rpc.Service == nil {
			continue
		}

		if graphqlCandidate, ok := rpc.Service.(commands.GraphQLAPI); ok {
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
