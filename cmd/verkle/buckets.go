package main

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
