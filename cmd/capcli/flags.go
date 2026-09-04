// Copyright 2026 The Erigon Authors
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

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"

	cliutils "github.com/erigontech/erigon/cmd/utils"
)

var (
	sentinelFlag = &cli.StringFlag{Name: "sentinel", Usage: "sentinel url", Value: "localhost:7777"}
	pprofFlag    = &cli.BoolFlag{Name: "pprof", Usage: "enable pprof"}
	toSlotFlag   = &cli.Uint64Flag{Name: "to", Usage: "slot to dump"}
)

func (c *chainCfg) fromCmd(cmd *cli.Command) { c.Chain = cmd.String(cliutils.ChainFlag.Name) }

// fromCmd keeps the existence check the kong `existingdir` type used to do:
// every command here reads an existing datadir, none creates one.
func (o *outputFolder) fromCmd(cmd *cli.Command) error {
	o.Datadir = cmd.String(cliutils.DataDirFlag.Name)
	st, err := os.Stat(o.Datadir)
	if err != nil {
		return fmt.Errorf("--datadir: %w", err)
	}
	if !st.IsDir() {
		return fmt.Errorf("--datadir: %q exists but is not a directory", o.Datadir)
	}
	return nil
}

func (w *withSentinel) fromCmd(cmd *cli.Command) { w.Sentinel = cmd.String(sentinelFlag.Name) }

func (w *withPPROF) fromCmd(cmd *cli.Command) { w.Pprof = cmd.Bool(pprofFlag.Name) }

func commands() []*cli.Command {
	return []*cli.Command{
		(&Chain{}).command(),
		(&DumpSnapshots{}).command(),
		(&CheckSnapshots{}).command(),
		(&LoopSnapshots{}).command(),
		(&RetrieveHistoricalState{}).command(),
		(&ChainEndpoint{}).command(),
		(&ArchiveSanitizer{}).command(),
		(&BenchmarkNode{}).command(),
		(&BlobArchiveStoreCheck{}).command(),
		(&DumpBlobsSnapshots{}).command(),
		(&CheckBlobsSnapshots{}).command(),
		(&CheckBlobsSnapshotsCount{}).command(),
		(&DumpBlobsSnapshotsToStore{}).command(),
		(&DumpStateSnapshots{}).command(),
		(&MakeDepositArgs{}).command(),
	}
}

func (c *Chain) command() *cli.Command {
	return &cli.Command{
		Name:  "chain",
		Usage: "download the entire chain from reqresp network",
		Flags: []cli.Flag{&cliutils.ChainFlag, sentinelFlag, &cliutils.DataDirFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			c.withSentinel.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			return c.Run(ctx)
		},
	}
}

func (c *DumpSnapshots) command() *cli.Command {
	return &cli.Command{
		Name:  "dump-snapshots",
		Usage: "generate caplin snapshots",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, toSlotFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			c.To = cmd.Uint64(toSlotFlag.Name)
			return c.Run(ctx)
		},
	}
}

func (c *CheckSnapshots) command() *cli.Command {
	return &cli.Command{
		Name:  "check-snapshots",
		Usage: "check snapshot folder against content of chain data",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, pprofFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			c.withPPROF.fromCmd(cmd)
			return c.Run(ctx)
		},
	}
}

func (c *LoopSnapshots) command() *cli.Command {
	slot := &cli.Uint64Flag{Name: "slot", Usage: "slot to check"}
	return &cli.Command{
		Name:  "loop-snapshots",
		Usage: "loop over snapshots",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, pprofFlag, slot},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			c.withPPROF.fromCmd(cmd)
			c.Slot = cmd.Uint64(slot.Name)
			return c.Run(ctx)
		},
	}
}

func (r *RetrieveHistoricalState) command() *cli.Command {
	compareFile := &cli.StringFlag{Name: "compare-file", Usage: "compare file"}
	compareSlot := &cli.Uint64Flag{Name: "compare-slot", Usage: "compare slot"}
	out := &cli.StringFlag{Name: "out", Usage: "output file"}
	return &cli.Command{
		Name:  "retrieve-historical-state",
		Usage: "retrieve historical state from db",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, pprofFlag, compareFile, compareSlot, out},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			r.chainCfg.fromCmd(cmd)
			if err := r.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			r.withPPROF.fromCmd(cmd)
			r.CompareFile = cmd.String(compareFile.Name)
			r.CompareSlot = cmd.Uint64(compareSlot.Name)
			r.Out = cmd.String(out.Name)
			return r.Run(ctx)
		},
	}
}

func (c *ChainEndpoint) command() *cli.Command {
	endpoint := &cli.StringFlag{Name: "endpoint", Usage: "endpoint"}
	blobs := &cli.BoolFlag{Name: "blobs", Usage: "also download blobs"}
	return &cli.Command{
		Name:  "chain-endpoint",
		Usage: "chain endpoint",
		Flags: []cli.Flag{endpoint, blobs, &cliutils.ChainFlag, &cliutils.DataDirFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.Endpoint = cmd.String(endpoint.Name)
			c.Blobs = cmd.Bool(blobs.Name)
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			return c.Run(ctx)
		},
	}
}

func (a *ArchiveSanitizer) command() *cli.Command {
	beaconAPIURL := &cli.StringFlag{Name: "beacon-api-url", Usage: "beacon api url", Value: "http://localhost:5555"}
	intervalSlot := &cli.Uint64Flag{Name: "interval-slot", Usage: "interval slot", Value: 19}
	startSlot := &cli.Uint64Flag{Name: "start-slot", Usage: "start slot"}
	faultOut := &cli.StringFlag{Name: "fault-out", Usage: "fault out"}
	return &cli.Command{
		Name:  "archive-sanitizer",
		Usage: "archive sanitizer",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, beaconAPIURL, intervalSlot, startSlot, faultOut},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			a.chainCfg.fromCmd(cmd)
			if err := a.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			a.BeaconApiURL = cmd.String(beaconAPIURL.Name)
			a.IntervalSlot = cmd.Uint64(intervalSlot.Name)
			a.StartSlot = cmd.Uint64(startSlot.Name)
			a.FaultOut = cmd.String(faultOut.Name)
			return a.Run(ctx)
		},
	}
}

func (b *BenchmarkNode) command() *cli.Command {
	baseURL := &cli.StringFlag{Name: "base-url", Usage: "base url", Value: "http://localhost:5555"}
	endpoint := &cli.StringFlag{Name: "endpoint", Usage: "endpoint", Value: "/eth/v1/beacon/states/{slot}/validators"}
	outCSV := &cli.StringFlag{Name: "out-csv", Usage: "output csv"}
	accept := &cli.StringFlag{Name: "accept", Usage: "accept", Value: "application/json"}
	head := &cli.BoolFlag{Name: "head", Usage: "head"}
	method := &cli.StringFlag{Name: "method", Usage: "method", Value: "GET"}
	body := &cli.StringFlag{Name: "body", Usage: "body", Value: "{}"}
	return &cli.Command{
		Name:  "benchmark-node",
		Usage: "benchmark node",
		Flags: []cli.Flag{&cliutils.ChainFlag, baseURL, endpoint, outCSV, accept, head, method, body},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			b.chainCfg.fromCmd(cmd)
			b.BaseURL = cmd.String(baseURL.Name)
			b.Endpoint = cmd.String(endpoint.Name)
			b.OutCSV = cmd.String(outCSV.Name)
			b.Accept = cmd.String(accept.Name)
			b.Head = cmd.Bool(head.Name)
			b.Method = cmd.String(method.Name)
			b.Body = cmd.String(body.Name)
			return b.Run(ctx)
		},
	}
}

func (b *BlobArchiveStoreCheck) command() *cli.Command {
	fromSlot := &cli.Uint64Flag{Name: "from-slot", Usage: "from slot"}
	return &cli.Command{
		Name:  "blob-archive-store-check",
		Usage: "blob archive store check",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, fromSlot},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			b.chainCfg.fromCmd(cmd)
			if err := b.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			b.FromSlot = cmd.Uint64(fromSlot.Name)
			return b.Run(ctx)
		},
	}
}

func (c *DumpBlobsSnapshots) command() *cli.Command {
	return &cli.Command{
		Name:  "dump-blobs-snapshots",
		Usage: "dump blobs snapshots",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, toSlotFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			c.To = cmd.Uint64(toSlotFlag.Name)
			return c.Run(ctx)
		},
	}
}

func (c *CheckBlobsSnapshots) command() *cli.Command {
	return &cli.Command{
		Name:  "check-blobs-snapshots",
		Usage: "check blobs snapshots",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, pprofFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			c.withPPROF.fromCmd(cmd)
			return c.Run(ctx)
		},
	}
}

func (c *CheckBlobsSnapshotsCount) command() *cli.Command {
	from := &cli.Uint64Flag{Name: "from", Usage: "from slot"}
	checkNeedRegen := &cli.BoolFlag{Name: "check-need-regen", Usage: "check if blobs need regen"}
	return &cli.Command{
		Name:  "check-blobs-snapshots-count",
		Usage: "check blobs snapshots count",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, pprofFlag, from, checkNeedRegen},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			c.withPPROF.fromCmd(cmd)
			c.From = cmd.Uint64(from.Name)
			c.CheckNeedRegen = cmd.Bool(checkNeedRegen.Name)
			return c.Run(ctx)
		},
	}
}

func (c *DumpBlobsSnapshotsToStore) command() *cli.Command {
	return &cli.Command{
		Name:  "dump-blobs-snapshots-to-store",
		Usage: "dump blobs snapshots to store",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, pprofFlag},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			c.withPPROF.fromCmd(cmd)
			return c.Run(ctx)
		},
	}
}

func (c *DumpStateSnapshots) command() *cli.Command {
	stepSize := &cli.Uint64Flag{Name: "step-size", Usage: "step size", Value: 10000}
	return &cli.Command{
		Name:  "dump-state-snapshots",
		Usage: "dump state snapshots",
		Flags: []cli.Flag{&cliutils.ChainFlag, &cliutils.DataDirFlag, toSlotFlag, stepSize},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			c.chainCfg.fromCmd(cmd)
			if err := c.outputFolder.fromCmd(cmd); err != nil {
				return err
			}
			c.To = cmd.Uint64(toSlotFlag.Name)
			c.StepSize = cmd.Uint64(stepSize.Name)
			return c.Run(ctx)
		},
	}
}

func (m *MakeDepositArgs) command() *cli.Command {
	privateKey := &cli.StringFlag{Name: "private-key", Usage: "private key to use for signing deposit"}
	withdrawalAddress := &cli.StringFlag{Name: "withdrawal-address", Usage: "withdrawal address to use for deposit"}
	amountEth := &cli.Uint64Flag{Name: "amount-eth", Usage: "amount of ETH to deposit", Value: 32}
	domainDeposit := &cli.StringFlag{Name: "domain-deposit", Usage: "domain for deposit signature", Value: "0x03000000"}
	genesisForkVersion := &cli.StringFlag{Name: "genesis-fork-version", Usage: "genesis fork version for deposit signature", Value: "0x00000000"}
	return &cli.Command{
		Name:  "make-deposit-args",
		Usage: "make deposit args",
		Flags: []cli.Flag{privateKey, withdrawalAddress, amountEth, domainDeposit, genesisForkVersion},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			m.PrivateKey = cmd.String(privateKey.Name)
			m.WithdrawalAddress = cmd.String(withdrawalAddress.Name)
			m.AmountEth = cmd.Uint64(amountEth.Name)
			m.DomainDeposit = cmd.String(domainDeposit.Name)
			m.GenesisForkVersion = cmd.String(genesisForkVersion.Name)
			return m.Run(ctx)
		},
	}
}
