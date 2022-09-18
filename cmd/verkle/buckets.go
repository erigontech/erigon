package main

import "github.com/ledgerwatch/erigon-lib/kv"

const (
	VerkleIncarnation            = "VerkleIncarnation"
	VerkleRoots                  = "VerkleRoots"
	PedersenHashedCodeLookup     = "PedersenHashedCodeLookup"
	PedersenHashedAccountsLookup = "PedersenHashedAccountsLookup"
	PedersenHashedStorageLookup  = "PedersenHashedStorageLookup"
	VerkleTrie                   = "VerkleTrie"
)

var ExtraBuckets = []string{
	VerkleIncarnation,
	PedersenHashedCodeLookup,
	PedersenHashedAccountsLookup,
	PedersenHashedStorageLookup,
	VerkleTrie,
	VerkleRoots,
}

func initDB(tx kv.RwTx) error {
	for _, b := range ExtraBuckets {
		if err := tx.CreateBucket(b); err != nil {
			return err
		}
	}
	return nil
}
