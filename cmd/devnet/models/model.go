package models

import "fmt"

const (
	// BuildDirArg is the build directory for the devnet executable
	BuildDirArg = "./build/bin/devnet"
	// DataDirArg is the datadir flag
	DataDirArg = "--datadir"
	// ChainArg is the chain flag
	ChainArg = "--chain"
	// DevPeriodArg is the dev.period flag
	DevPeriodArg = "--dev.period"
	// VerbosityArg is the verbosity flag
	VerbosityArg = "--verbosity"
	// Mine is the mine flag
	Mine = "--mine"

	// DataDirParam is the datadir parameter
	DataDirParam = "./dev"
	// ChainParam is the chain parameter
	ChainParam = "dev"
	// DevPeriodParam is the dev.period parameter
	DevPeriodParam = "30"
	// VerbosityParam is the verbosity parameter
	VerbosityParam = "3"
)

// ParameterFromArgument merges the argument and parameter and returns a flag input string
func ParameterFromArgument(arg, param string) (string, error) {
	if arg == "" {
		return "", ErrInvalidArgument
	}
	return fmt.Sprintf("%s=%s", arg, param), nil
}
