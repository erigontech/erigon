package sszql

import "strconv"

func parseQuery(request SSZQLRequest, version int, block_id string) SSZQLResponse {

	var response SSZQLResponse

	parseQueries(request, &response)

	parseAliases(request.Aliases, &response)

	if request.IncludeProofs {
		generateProof(&response)
	}

	return response
}

func parseQueries(req SSZQLRequest, res *SSZQLResponse) []SSZQuery {
	for i, query := range req.Queries {
		res.Paths = append(res.Paths, query.Path)
		res.Results = append(res.Results, Result("query "+strconv.Itoa(i)+" result"))
	}

	return req.Queries
}

func parseFilters(filters Filter) []string {
	var ret []string
	return ret
}

func parseAliases(aliases []Alias, res *SSZQLResponse) map[string]string {
	m := make(map[string]string)

	for _, alias := range aliases {
		value := parseQueryWithPathAndFilter(alias.Path, alias.Filter)
		m[alias.Alias] = value
		res.Aliases = append(res.Aliases, AliasResponse{Alias: alias.Alias, Value: value})
	}

	return m
}

// todo: implement actual logic
func parseQueryWithPathAndFilter(path Path, filter Filter) string {
	return "dummy"
}

func generateProof(res *SSZQLResponse) []Proof {
	proofs := make([]Proof, 0, len(res.Results))
	for i := range res.Results {
		proof := Proof("proof of query" + strconv.Itoa(i))
		proofs = append(proofs, proof)
		res.Proofs = append(res.Proofs, proof)
	}
	return proofs
}
