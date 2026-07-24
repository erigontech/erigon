package sszql

func parseQuery(request SSZQLRequest, version int, block_id string) SSZQLResponse {

	var response SSZQLResponse

	for _, query := range request.Queries {
		response.Paths = append(response.Paths, query.Path)
	}

	return response
}
