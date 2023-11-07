package tracing

import (
	"github.com/urfave/cli/v2"
)

var (
	TracerFlag = cli.StringFlag{
		Name:  "vmtrace",
		Usage: "Set the provider tracer",
	}

	TracerConfigFlag = cli.StringFlag{
		Name:  "vmtrace.config",
		Usage: "Set the config of the tracer",
	}
)

var Flags = []cli.Flag{
	&TracerFlag,
	&TracerConfigFlag,
}
