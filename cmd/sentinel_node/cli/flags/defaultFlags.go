package utils

import "github.com/urfave/cli"

var LightClientDefaultFlags = []cli.Flag{
	LightClientPort,
	LightClientAddr,
}
