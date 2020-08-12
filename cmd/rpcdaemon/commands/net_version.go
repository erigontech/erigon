package commands

import (
	"context"
)

func (api *APIImpl) NetVersion(_ context.Context) uint64 {
	return api.ethBackend.NetVersion()
}
