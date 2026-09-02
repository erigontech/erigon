package sszql

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/erigontech/erigon/rpc"
)

var errBadQuery = errors.New("bad query")

func parseQueryV1(request SSZQLRequest, blockID rpc.BlockNumberOrHash) (SSZQLResponse, error) {
	response := SSZQLResponse{
		Paths:    make([]Path, 0),
		Gindices: make([]Gindex, 0),
		Leaves:   make([]Leaf, 0),
		Results:  make([]Result, 0),
	}
	aliases, err := parseAliases(request.Aliases, &response, blockID)
	if err != nil {
		return SSZQLResponse{}, err
	}
	if err := parseQueries(request, &response, blockID, aliases); err != nil {
		return SSZQLResponse{}, err
	}
	if request.IncludeProofs {
		if err := generateProof(&response); err != nil {
			return SSZQLResponse{}, err
		}
	}

	return response, nil
}

func parseQueries(req SSZQLRequest, res *SSZQLResponse, blockID rpc.BlockNumberOrHash, aliases map[string]string) error {
	for _, query := range req.Queries {
		resolvedPath, err := resolvePath(query.Path, query.Anchor, blockID)
		if err != nil {
			return err
		}
		res.Paths = append(res.Paths, query.Path)
		res.Results = append(res.Results, resolvedPath.Value)
		res.Gindices = append(res.Gindices, resolvedPath.Gindex)
		res.Leaves = append(res.Leaves, resolvedPath.Leaf)
	}

	return nil
}

func parseAliases(aliases []Alias, res *SSZQLResponse, blockID rpc.BlockNumberOrHash) (map[string]string, error) {
	m := make(map[string]string)

	for _, alias := range aliases {
		if _, dup := m[alias.Alias]; dup {
			return nil, fmt.Errorf("%w: duplicate alias %q", errBadQuery, alias.Alias)
		}

		resolvedPath, err := resolvePath(alias.Path, alias.Anchor, blockID)
		if err != nil {
			return nil, err
		}
		m[alias.Alias] = string(resolvedPath.Value)
		res.Aliases = append(res.Aliases, AliasResponse{Alias: alias.Alias, Value: string(resolvedPath.Value)})
	}

	return m, nil
}

func resolvePath(path Path, anchor Anchor, blockID rpc.BlockNumberOrHash) (ResolvedPath, error) {
	response := ResolvedPath{
		Gindex: Gindex(99),
		Leaf:   Leaf("0xabcdef"),
		Value:  Result("0xabcdef"),
	}
	return response, nil
}

func generateProof(res *SSZQLResponse) error {
	for i := range res.Results {
		res.Proofs = append(res.Proofs, Proof("proof of query"+strconv.Itoa(i)))
	}
	return nil
}
