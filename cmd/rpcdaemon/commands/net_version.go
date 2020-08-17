package commands

import (
	"context"
)

func (api *NetAPIImpl) Version(_ context.Context) uint64 {
	return api.ethBackend.NetVersion()
}
