package main

import "github.com/ledgerwatch/erigon-lib/kv"

const (
	PedersenHashedAccounts       = "PedersenHashedAccounts"
	PedersenHashedStorage        = "PedersenHashedStorage"
	PedersenHashedCode           = "PedersenHashedCode"
	PedersenHashedCodeLookup     = "PedersenHashedCodeLookup"
	PedersenHashedAccountsLookup = "PedersenHashedAccountsLookup"
	PedersenHashedStorageLookup  = "PedersenHashedStorageLookup"
	VerkleTrie                   = "VerkleTrie"
)

var ExtraBuckets = []string{
	PedersenHashedAccounts,
	PedersenHashedStorage,
	PedersenHashedCode,
	PedersenHashedCodeLookup,
	PedersenHashedAccountsLookup,
	PedersenHashedStorageLookup,
	VerkleTrie,
}

func initDB(tx kv.RwTx) error {
	for _, b := range ExtraBuckets {
		if err := tx.CreateBucket(b); err != nil {
			return err
		}
	}
	return nil
}
