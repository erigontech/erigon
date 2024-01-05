package sequencer

import "os"

const (
	// Env variable to enable sequencer
	SEQUENCER_ENV_KEY = "CDK_ERIGON_SEQUENCER"
)

func IsSequencer() bool {
	// TODO: SEQ: make a commmand-line flag for that and replace the env variable
	// read from the environment
	return os.Getenv(SEQUENCER_ENV_KEY) == "1"
}
