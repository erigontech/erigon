package flags

import "github.com/urfave/cli/v2"

var (
	SegTypes = cli.StringSliceFlag{
		Name:     "types",
		Usage:    `Segment types to comparre with optional e.g. headers,bodies,transactions`,
		Required: false,
	}
)
