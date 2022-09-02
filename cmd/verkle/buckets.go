package main

const (
	PedersenHashedAccounts       = "PedersenHashedAccounts"
	PedersenHashedStorage        = "PedersenHashedStorage"
	PedersenHashedCode           = "PedersenHashedCode"
	PedersenHashedAccountsLookup = "PedersenHashedAccountsLookup"
	PedersenHashedStorageLookup  = "PedersenHashedStorageLookup"
)

var ExtraBuckets = []string{
	PedersenHashedAccounts,
	PedersenHashedStorage,
	PedersenHashedCode,
	PedersenHashedAccountsLookup,
	PedersenHashedStorageLookup,
}
