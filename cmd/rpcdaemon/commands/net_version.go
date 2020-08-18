package commands

import (
	"context"
	"strconv"
)

func (api *NetAPIImpl) Version(_ context.Context) (string, error) {
	res, err := api.ethBackend.NetVersion()
	if err != nil {
		return "", err
	}
	return strconv.FormatUint(res, 10), nil
}
