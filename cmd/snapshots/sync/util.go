package sync

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/downloader"
)

func CheckRemote(rcCli *downloader.RCloneClient, src string) error {

	remotes, err := rcCli.ListRemotes(context.Background())

	if err != nil {
		return err
	}

	hasRemote := false

	for _, remote := range remotes {
		if src == remote {
			hasRemote = true
			break
		}
	}

	if !hasRemote {
		return fmt.Errorf("unknown remote: %s", src)
	}

	return nil
}
