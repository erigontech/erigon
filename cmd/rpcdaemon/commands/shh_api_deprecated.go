package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

// SHHAPI the interface for the shh_ RPC commands (deprecated)
type SHHAPI interface {
	Post(_ context.Context, _ SHHPost) (bool, error)
	Version(_ context.Context) (string, error)
	NewIdentity(_ context.Context) (string, error)
	HasIdentity(_ context.Context, _ string) (bool, error)
	NewGroup(_ context.Context) (string, error)
	AddToGroup(_ context.Context, _ string) (bool, error)
	NewFilter(_ context.Context, _ SHHFilter) (hexutil.Uint, error)
	UninstallFilter(_ context.Context, _ hexutil.Uint) (bool, error)
	GetFilterChanges(_ context.Context, _ hexutil.Uint) ([]string, error)
	GetMessages(_ context.Context, _ hexutil.Uint) ([]string, error)
}

// SHHAPIImpl data structure to store things needed for shh_ commands
type SHHAPIImpl struct {
	unused uint64
}

// NewSHHAPIImpl returns NetAPIImplImpl instance
func NewSHHAPIImpl() *SHHAPIImpl {
	return &SHHAPIImpl{
		unused: uint64(0),
	}
}

// SHHPost type for shh_post command (deprecated)
type SHHPost struct {
	_ string       // from
	_ string       // to
	_ []string     // topics
	_ string       // payload
	_ hexutil.Uint // priority
	_ hexutil.Uint // ttl
}

// Post implements shh_post. Sends a whisper message.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) Post(_ context.Context, _ SHHPost) (bool, error) {
	return false, fmt.Errorf(NotAvailableDeprecated, "shh_post")
}

// Version implements shh_version. Returns the current whisper protocol version.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) Version(_ context.Context) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_version")
}

// NewIdentity implements shh_newIdentity. Creates new whisper identity in the client.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) NewIdentity(_ context.Context) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_newIdentity")
}

// HasIdentity implements shh_hasIdentity. Checks if the client hold the private keys for a given identity.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) HasIdentity(_ context.Context, _ string) (bool, error) {
	return false, fmt.Errorf(NotAvailableDeprecated, "shh_hasIdentity")
}

// NewGroup implements shh_newGroup. Create a new group.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) NewGroup(_ context.Context) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_newGroup")
}

// AddToGroup implements shh_addToGroup. Add to a group.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) AddToGroup(_ context.Context, _ string) (bool, error) {
	return false, fmt.Errorf(NotAvailableDeprecated, "shh_addToGroup")
}

// SHHFilter type for shh_newFilter command
type SHHFilter struct {
	_ string
	_ []string
}

// NewFilter implements shh_newFilter. Creates filter to notify, when client receives whisper message matching the filter options.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) NewFilter(_ context.Context, _ SHHFilter) (hexutil.Uint, error) {
	return hexutil.Uint(0), fmt.Errorf(NotAvailableDeprecated, "shh_newFilter")
}

// UninstallFilter implements shh_uninstallFilter. Uninstalls a filter with given id.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) UninstallFilter(_ context.Context, _ hexutil.Uint) (bool, error) {
	return false, fmt.Errorf(NotAvailableDeprecated, "shh_uninstallFilter")
}

// GetFilterChanges implements shh_getFilterChanges. Polling method for whisper filters. Returns new messages since the last call of this method.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) GetFilterChanges(_ context.Context, _ hexutil.Uint) ([]string, error) {
	return []string{}, fmt.Errorf(NotAvailableDeprecated, "shh_getFilterChanges")
}

// GetMessages implements shh_getMessages. Get all messages matching a filter. Unlike shh_getFilterChanges this returns all messages.
// Deprecated: This function will be removed in the future.
func (api *SHHAPIImpl) GetMessages(_ context.Context, _ hexutil.Uint) ([]string, error) {
	return []string{}, fmt.Errorf(NotAvailableDeprecated, "shh_getMessages")
}
