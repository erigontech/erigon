package arbitrum

// PublicNetAPI offers network related RPC methods
type PublicTxPoolAPI struct{}

// NewPublicNetAPI creates a new net API instance.
func NewPublicTxPoolAPI() *PublicTxPoolAPI {
	return &PublicTxPoolAPI{}
}

// Version returns the current ethereum protocol version.
func (s *PublicTxPoolAPI) Content() map[string]map[string]map[string]*struct{} {
	return map[string]map[string]map[string]*struct{}{
		"pending": make(map[string]map[string]*struct{}),
		"queued":  make(map[string]map[string]*struct{}),
	}
}
