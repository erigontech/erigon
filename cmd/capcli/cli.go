package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/rpc"
	"github.com/spf13/afero"
	"google.golang.org/grpc"
)

var CLI struct {
	Migrate Migrate `cmd:"" help:"migrate from one state to another"`

	Blocks Blocks `cmd:"" help:"download blocks from gossip network"`
}

type chainCfg struct {
	Chain string `help:"chain" default:"mainnet"`
}

func (c *chainCfg) configs() (beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig, err error) {
	genesisConfig, _, beaconConfig, _, err = clparams.GetConfigsByNetworkName(c.Chain)
	return
}

type outputFolder struct {
	Output string `help:"where to output to, defaults to current directory" default:"." short:"o"`
}

type withSentinel struct {
	Sentinel string `help:"sentinel url" default:"localhost:7777"`
}

func (w *withSentinel) connectSentinel() (sentinel.SentinelClient, error) {
	gconn, err := grpc.Dial(w.Sentinel, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return sentinel.NewSentinelClient(gconn), nil
}

func openFs(fsName string) (afero.Fs, error) {
	return afero.NewBasePathFs(afero.NewOsFs(), fsName), nil
}

type Blocks struct {
	chainCfg
	outputFolder
	withSentinel

	FromBlock int `arg:"" name:"from" default:"0"`
	ToBlock   int `arg:"" name:"to" default:"-1"`
}

func (b *Blocks) Run(ctx *Context) error {
	s, err := b.withSentinel.connectSentinel()
	if err != nil {
		return err
	}
	beaconConfig, genesisConfig, err := b.configs()
	if err != nil {
		return err
	}
	beacon := rpc.NewBeaconRpcP2P(ctx, s, beaconConfig, genesisConfig)
	resp, _, err := beacon.SendBeaconBlocksByRangeReq(ctx, uint64(b.FromBlock), uint64(b.ToBlock))
	if err != nil {
		return fmt.Errorf("error get beacon blocks: %w", err)
	}
	d, err := openFs(b.Output)
	if err != nil {
		return err
	}
	d.MkdirAll("", 0o640)
	for _, vv := range resp {
		v := vv
		err := func() error {
			fname := fmt.Sprintf("%16x_b.ssz", v.Block.Slot)
			info, err := d.Stat(fname)
			if err == nil {
				if info.Size() > 0 {
					fmt.Fprintf(os.Stderr, "skipping %s since non 0 file\n", fname)
				}
			}
			bts, err := v.EncodeSSZ(nil)
			if err != nil {
				return err
			}
			fp, err := d.OpenFile(fname, os.O_CREATE|os.O_TRUNC, 0o640)
			if err != nil {
				return err
			}
			defer fp.Close()
			err = fp.Truncate(0)
			if err != nil {
				return err
			}
			_, err = fp.Write(bts)
			if err != nil {
				return err
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

type Migrate struct {
	outputFolder

	State  string   `arg:"" help:"state to start from (can be url to checkpoint)"`
	Blocks []string `arg:"" name:"blocks" help:"blocks to migrate, in order" type:"path"`
}

func (m *Migrate) Run(ctx *Context) error {
	return nil
}
