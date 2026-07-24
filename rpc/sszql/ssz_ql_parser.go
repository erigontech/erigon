package sszql

func parseQuery(request SSZQLRequest, version int, block_id string) SSZQLResponse {

	var response SSZQLResponse

	for _, query := range request.Queries {
		response.Paths = append(response.Paths, query.Path)
		// filter := parseFilters(query.Filters)
	}

	alias_map := parseAlias(request.Aliases)

	for k, v := range alias_map {
		response.Aliases = append(response.Aliases, AliasResponse{Alias: k, Value: v})
	}

	return response
}

func parseFilters(filters Filter) []string {
	var ret []string
	return ret
}

func parseAlias(aliases []Aliases) map[string]string {
	m := make(map[string]string)

	for _, alias := range aliases {
		value := parseQueryWithPathAndFilter(alias.Path, alias.Filters)
		m[alias.Alias] = value
	}

	return m
}

func parseQueryWithPathAndFilter(path Path, filter Filter) string {
	return "dummy"
}
