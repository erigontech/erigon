package testutil

import (
	"os"

	"github.com/erigontech/erigon/execution/chain"
)

func UseGevm() bool {
	return os.Getenv("USE_GEVM") == "1"
}

func gevmTesterSupported(config *chain.Config) bool {
	return config != nil && config.AmsterdamTime == nil
}
