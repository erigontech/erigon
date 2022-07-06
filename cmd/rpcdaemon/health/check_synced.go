package health

import (
	"errors"
	"net/http"

	"github.com/ledgerwatch/log/v3"
)

var (
	errNotSynced = errors.New("not synced")
)

func checkSynced(ethAPI EthAPI, r *http.Request) error {
	i, err := ethAPI.Syncing(r.Context())
	if err != nil {
		log.Root().Warn("unable to process synced request", "err", err.Error())
		return err
	}
	if i == nil || i == false {
		return nil
	}

	return errNotSynced
}
