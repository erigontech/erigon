package model

// Account represents an Ethereum account in the GraphQL schema.
// BlockNum is an internal field not part of the GraphQL schema; it carries
// the block context to the storage resolver so each Account.storage(slot)
// call targets the correct block.
type Account struct {
	Address          string `json:"address"`
	Balance          string `json:"balance"`
	TransactionCount uint64 `json:"transactionCount"`
	Code             string `json:"code"`
	Storage          string `json:"storage"`
	BlockNum         uint64 `json:"-"`
}

// NewAccountAtBlock returns an Account pre-populated with the given block number.
// Use this when constructing address-only Account stubs within a block resolver so
// that downstream storage(slot) calls query the right block.
func NewAccountAtBlock(blockNum uint64) *Account {
	return &Account{BlockNum: blockNum}
}
