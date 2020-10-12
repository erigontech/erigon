package commands

import (
	"context"
	"fmt"
)

// GetCompilers Returns a list of available compilers in the client. (deprecated)
func (api *APIImpl) GetCompilers(_ context.Context) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "eth_getCompilers")
}

// CompileLLL Returns compiled LLL code. (deprecated)
func (api *APIImpl) CompileLLL(_ context.Context, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "eth_compileLLL")
}

// CompileSolidity Returns compiled solidity code. (deprecated)
func (api *APIImpl) CompileSolidity(ctx context.Context, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "eth_compileSolidity")
}

// CompileSerpent Returns compiled serpent code. (deprecated)
func (api *APIImpl) CompileSerpent(ctx context.Context, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "eth_compileSerpent")
}

// Accounts Returns a list of addresses owned by client. (deprecated)
func (api *APIImpl) Accounts(ctx context.Context) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "eth_accounts")
}

// Sign The sign method calculates an Ethereum specific signature. (deprecated)
func (api *APIImpl) Sign(ctx context.Context, _ string, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "eth_sign")
}
