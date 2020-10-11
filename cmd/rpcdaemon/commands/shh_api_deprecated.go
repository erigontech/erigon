package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

// SHHAPI the interface for the shh_ RPC commands (deprecated)
type SHHAPI interface {
	Post(_ context.Context, _ SHHPost) (string, error)
	Version(_ context.Context) (string, error)
	NewIdentity(_ context.Context) (string, error)
	HasIdentity(_ context.Context, ident string) (string, error)
	NewGroup(_ context.Context) (string, error)
	AddToGroup(_ context.Context, grp string) (string, error)
	NewFilter(_ context.Context, _ SHHFilter) (string, error)
	UninstallFilter(_ context.Context, filterID hexutil.Uint) (string, error)
	GetFilterChanges(_ context.Context, filterID hexutil.Uint) (string, error)
	GetMessages(_ context.Context, filterID hexutil.Uint) (string, error)
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

// SHHPost type for shh_post command
type SHHPost struct {
	_ string
	_ []string
	_ string
	_ hexutil.Uint
	_ hexutil.Uint
}

// Post Sends a whisper message (deprecated)
func (api *SHHAPIImpl) Post(_ context.Context, _ SHHPost) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_post")
}

// Version Returns the current whisper protocol version (deprecated)
func (api *SHHAPIImpl) Version(_ context.Context) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_version")
}

// NewIdentity Creates new whisper identity in the client (deprecated)
func (api *SHHAPIImpl) NewIdentity(_ context.Context) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_newIdentity")
}

// HasIdentity Checks if the client hold the private keys for a given identity (deprecated)
func (api *SHHAPIImpl) HasIdentity(_ context.Context, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_hasIdentity")
}

// NewGroup (?) (deprecated)
func (api *SHHAPIImpl) NewGroup(_ context.Context) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_newGroup")
}

// AddToGroup (?) (deprecated)
func (api *SHHAPIImpl) AddToGroup(_ context.Context, _ string) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_addToGroup")
}

// SHHFilter type for shh_newFilter command
type SHHFilter struct {
	_ string
	_ []string
}

// NewFilter Creates filter to notify, when client receives whisper message matching the filter options (deprecated)
func (api *SHHAPIImpl) NewFilter(_ context.Context, _ SHHFilter) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_newFilter")
}

// UninstallFilter Uninstalls a filter with given id (deprecated)
func (api *SHHAPIImpl) UninstallFilter(_ context.Context, _ hexutil.Uint) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_uninstallFilter")
}

// GetFilterChanges Polling method for whisper filters (deprecated)
func (api *SHHAPIImpl) GetFilterChanges(_ context.Context, _ hexutil.Uint) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_getFilterChanges")
}

// GetMessages Get all messages matching a filter (deprecated)
func (api *SHHAPIImpl) GetMessages(_ context.Context, _ hexutil.Uint) (string, error) {
	return "", fmt.Errorf(NotAvailableDeprecated, "shh_getMessages")
}
