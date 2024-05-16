package tracing

import (
	"github.com/urfave/cli/v2"
)

var (
	VMTraceFlag = cli.StringFlag{
		Name:  "vmtrace",
		Usage: "Set the provider tracer",
	}

	VMTraceJsonConfigFlag = cli.StringFlag{
		Name:  "vmtrace.jsonconfig",
		Usage: "Set the config of the tracer",
	}
)

var Flags = []cli.Flag{
	&VMTraceFlag,
	&VMTraceJsonConfigFlag,
}
