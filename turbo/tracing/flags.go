package tracing

import (
	"github.com/urfave/cli/v2"
)

var (
	TracerFlag = cli.StringFlag{
		Name:  "tracer",
		Usage: "Set the provider tracer",
	}

	TracerConfigFlag = cli.StringFlag{
		Name:  "tracer.config",
		Usage: "Set the config of the tracer",
	}
)

var Flags = []cli.Flag{
	&TracerFlag,
	&TracerConfigFlag,
}
