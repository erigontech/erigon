package testutil

import "os"

func useGevmFromEnv() bool {
	return os.Getenv("USE_GEVM") == "1"
}
