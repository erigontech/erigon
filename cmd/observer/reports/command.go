// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package reports

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/v3/cmd/utils"
)

type CommandFlags struct {
	DataDir      string
	Chain        string
	ClientsLimit uint
	MaxPingTries uint
	Estimate     bool

	SentryCandidates bool
	ErigonLogPath    string
}

type Command struct {
	command cobra.Command
	flags   CommandFlags
}

func NewCommand() *Command {
	command := cobra.Command{
		Use:   "report",
		Short: "P2P network crawler database report",
	}

	instance := Command{
		command: command,
	}
	instance.withDatadir()
	instance.withChain()
	instance.withClientsLimit()
	instance.withMaxPingTries()
	instance.withEstimate()
	instance.withSentryCandidates()
	instance.withErigonLogPath()

	return &instance
}

func (command *Command) withDatadir() {
	flag := utils.DataDirFlag
	command.command.Flags().StringVar(&command.flags.DataDir, flag.Name, flag.Value.String(), flag.Usage)
	must(command.command.MarkFlagDirname(utils.DataDirFlag.Name))
}

func (command *Command) withChain() {
	flag := utils.ChainFlag
	command.command.Flags().StringVar(&command.flags.Chain, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withClientsLimit() {
	flag := cli.UintFlag{
		Name:  "clients-limit",
		Usage: "A number of top clients to show",
		Value: uint(10),
	}
	command.command.Flags().UintVar(&command.flags.ClientsLimit, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withMaxPingTries() {
	flag := cli.UintFlag{
		Name:  "max-ping-tries",
		Usage: "A number of PING failures for a node to be considered dead",
		Value: 3,
	}
	command.command.Flags().UintVar(&command.flags.MaxPingTries, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) withEstimate() {
	flag := cli.BoolFlag{
		Name:  "estimate",
		Usage: "Estimate totals including nodes that replied with 'too many peers'",
	}
	command.command.Flags().BoolVar(&command.flags.Estimate, flag.Name, false, flag.Usage)
}

func (command *Command) withSentryCandidates() {
	flag := cli.BoolFlag{
		Name:  "sentry-candidates",
		Usage: "Count unseen peers. Requires 'erigon-log'.",
	}
	command.command.Flags().BoolVar(&command.flags.SentryCandidates, flag.Name, false, flag.Usage)
}

func (command *Command) withErigonLogPath() {
	flag := cli.StringFlag{
		Name:  "erigon-log",
		Usage: "Erigon log file path",
	}
	command.command.Flags().StringVar(&command.flags.ErigonLogPath, flag.Name, flag.Value, flag.Usage)
}

func (command *Command) RawCommand() *cobra.Command {
	return &command.command
}

func (command *Command) OnRun(runFunc func(ctx context.Context, flags CommandFlags) error) {
	command.command.RunE = func(cmd *cobra.Command, args []string) error {
		return runFunc(cmd.Context(), command.flags)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
