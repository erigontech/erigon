package sync

import "github.com/ledgerwatch/erigon/core/types"

type BodiesVerifier func(headers []*types.Header, bodies []*types.Body) error

func VerifyBodies(_ []*types.Header, _ []*types.Body) error {
	//
	// TODO
	//
	return nil
}
