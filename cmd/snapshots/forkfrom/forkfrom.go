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

// Package forkfrom implements the `erigon snapshots fork-from` CLI
// subcommand. It produces the EL-side artefacts for a shadow-fork: a
// derived chain.Config + a copy of the parent's pre-cut snapshot
// files in a fresh datadir.
//
// The CL-side artefacts (config.yaml + genesis.ssz + validator-set
// mnemonic) come from Phase 2c-CL — fork-from emits EL-only today; a
// fork created with this command requires Phase 2c-CL's CL artefacts
// before it can actually advance past the cut block (post-merge update
// processing is driven by the CL via Engine API).
package forkfrom

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/log/v3"
	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/execution/chain"
)

// CLI flags. Each flag is one input the operator must supply (or an
// optional override). Required flags are marked Required: true so
// urfave/cli errors at parse time rather than at execution.

var ParentRPCFlag = cli.StringFlag{
	Name:  "parent-rpc",
	Usage: "JSON-RPC endpoint of the parent chain (live-capture mode). Mutually exclusive with --parent-cut-file.",
}

var ParentCutFileFlag = cli.StringFlag{
	Name:  "parent-cut-file",
	Usage: "Path to a previously-captured parent-cut.json (frozen-file mode). Mutually exclusive with --parent-rpc.",
}

var ParentChainFlag = cli.StringFlag{
	Name:  "parent-chain",
	Usage: `Parent chain name (e.g. "mainnet", "sepolia"). Required in live-capture mode; ignored in frozen-file mode.`,
}

var ParentDatadirFlag = cli.StringFlag{
	Name:     "parent-datadir",
	Usage:    "Path to the parent's datadir. fork-from reads chain.json + the snap dir from here.",
	Required: true,
}

var CutBlockFlag = cli.Uint64Flag{
	Name:  "cut-block",
	Usage: "EL block number at which the fork diverges. 0 means 'latest' (live mode only). Required in live mode.",
}

var NewChainNameFlag = cli.StringFlag{
	Name:     "new-chain-name",
	Usage:    `Fork's human-readable name (e.g. "mainnet-fork-23760000"). Becomes the derived chain.Config.ChainName.`,
	Required: true,
}

var NewDatadirFlag = cli.StringFlag{
	Name:     "new-datadir",
	Usage:    "Path to the fork's fresh datadir. fork-from refuses to clobber a non-empty existing dir.",
	Required: true,
}

var ParentManifestHashFlag = cli.StringFlag{
	Name:  "parent-manifest-hash",
	Usage: "Hex (40-char) info-hash of the parent's V2 manifest at cut time. Optional override; otherwise empty in the parent-cut.json.",
}

var ParentManifestNameFlag = cli.StringFlag{
	Name:  "parent-manifest-name",
	Usage: "On-disk name of the parent's V2 manifest at cut time (e.g. chain.v2.<enr-fp>.<seq>.toml). Optional.",
}

var SaveParentCutFlag = cli.StringFlag{
	Name:  "save-parent-cut",
	Usage: "Write the captured parent-cut.json to this path. Useful for replayable / shareable cuts.",
}

var DryRunFlag = cli.BoolFlag{
	Name:  "dry-run",
	Usage: "Print the file-copy plan + classifications; do NOT write the fork datadir.",
}

// Command is the urfave/cli registration. Wire into cmd/snapshots/main.go's
// app.Commands.
var Command = cli.Command{
	Name:  "fork-from",
	Usage: "Create the EL-side artefacts for a shadow-fork (derived chain.Config + pre-cut snapshot copy into a fresh datadir)",
	Description: `
fork-from produces the EL artefacts needed for a shadow-fork:

  1. Capture the parent's cut point — either live via --parent-rpc OR
     from a previously-captured frozen file via --parent-cut-file.
  2. Derive the fork's chain.Config from the parent's chain.json: keeps
     parent's ChainID (replay protection), preserves activated forks
     through the cut, drops future activations, populates the fork-
     lineage fields (Parent, CutBlock, ParentManifestHash).
  3. Plan the pre-cut file copy from --parent-datadir's snap dir.
     Files entirely before the cut are copied; files that straddle the
     cut are excluded (the fork retires them fresh); post-cut files
     are skipped.
  4. Write the new datadir at --new-datadir: chain.json + copied snap
     files. Refuses to clobber a non-empty existing dir.

CL-side artefacts (config.yaml, genesis.ssz, validator-set mnemonic)
are out of scope for this command; a fork created here cannot advance
past the cut block until paired with the CL setup (Phase 2c-CL).
`,
	Flags: []cli.Flag{
		&ParentRPCFlag,
		&ParentCutFileFlag,
		&ParentChainFlag,
		&ParentDatadirFlag,
		&CutBlockFlag,
		&NewChainNameFlag,
		&NewDatadirFlag,
		&ParentManifestHashFlag,
		&ParentManifestNameFlag,
		&SaveParentCutFlag,
		&DryRunFlag,
	},
	Action: action,
}

func action(ctx *cli.Context) error {
	logger := log.New("cmd", "fork-from")

	rpcURL := ctx.String(ParentRPCFlag.Name)
	cutFile := ctx.String(ParentCutFileFlag.Name)
	if (rpcURL == "") == (cutFile == "") {
		return fmt.Errorf("exactly one of --%s or --%s is required", ParentRPCFlag.Name, ParentCutFileFlag.Name)
	}

	// Resolve the cut point — capture-live or load-frozen.
	var cut *dl.ParentCut
	var err error
	if rpcURL != "" {
		parentChain := ctx.String(ParentChainFlag.Name)
		if parentChain == "" {
			return fmt.Errorf("--%s is required in live-capture mode", ParentChainFlag.Name)
		}
		cutBlock := ctx.Uint64(CutBlockFlag.Name)
		cut, err = dl.CaptureParentCut(
			ctx.Context, rpcURL, parentChain, cutBlock,
			ctx.String(ParentManifestNameFlag.Name),
			ctx.String(ParentManifestHashFlag.Name),
			logger,
		)
		if err != nil {
			return fmt.Errorf("capture parent cut: %w", err)
		}
		if savePath := ctx.String(SaveParentCutFlag.Name); savePath != "" {
			if err := dl.SaveParentCut(savePath, cut); err != nil {
				return fmt.Errorf("save parent-cut to %s: %w", savePath, err)
			}
			logger.Info("[fork-from] captured parent-cut saved", "path", savePath)
		}
	} else {
		cut, err = dl.LoadParentCut(cutFile)
		if err != nil {
			return fmt.Errorf("load parent-cut from %s: %w", cutFile, err)
		}
	}
	logger.Info("[fork-from] parent cut",
		"chain", cut.ParentChain,
		"chain_id", cut.ParentChainID,
		"cut_block", cut.CutBlock,
		"cut_block_hash", cut.CutBlockHash.Hex(),
		"cut_block_timestamp", cut.CutBlockTimestamp,
		"source", string(cut.Source))

	// Load the parent's chain.Config.
	parentDatadir := ctx.String(ParentDatadirFlag.Name)
	parentConfigPath := parentDatadir + "/chain.json"
	parentConfig, err := loadChainConfig(parentConfigPath)
	if err != nil {
		return fmt.Errorf("load parent chain.json: %w", err)
	}

	// Derive the fork's chain.Config.
	forkName := ctx.String(NewChainNameFlag.Name)
	derived, err := dl.DeriveForkChainConfig(parentConfig, cut, forkName)
	if err != nil {
		return fmt.Errorf("derive fork chain.Config: %w", err)
	}
	logger.Info("[fork-from] derived fork chain.Config",
		"chain_name", derived.ChainName,
		"chain_id", derived.ChainID,
		"parent", derived.Parent,
		"cut_block", derived.CutBlock,
		"parent_manifest_hash", hex.EncodeToString(derived.ParentManifestHash[:]))

	// Plan the pre-cut file copy. v1: no step→block map yet — state
	// files default to "straddle" if their step boundary isn't
	// explicitly mapped. Operators get a visible list of straddles
	// to make this less surprising.
	parentSnapDir := parentDatadir + "/snapshots"
	plan, err := dl.BuildCopyPlan(parentSnapDir, cut.CutBlock, dl.StepToBlock{})
	if err != nil {
		return fmt.Errorf("build copy plan: %w", err)
	}
	logger.Info("[fork-from] copy plan",
		"copy", len(plan.Copy),
		"straddle", len(plan.Straddle),
		"post_cut", len(plan.PostCut),
		"errors", len(plan.Errors))
	for _, e := range plan.Straddle {
		logger.Info("[fork-from] file straddles cut (fork retires fresh)", "name", e.RelPath, "reason", e.Reason)
	}
	for _, errStr := range plan.Errors {
		logger.Warn("[fork-from] classification error", "err", errStr)
	}

	if ctx.Bool(DryRunFlag.Name) {
		logger.Info("[fork-from] dry-run: not writing fork datadir")
		return nil
	}

	// Write the fork datadir.
	newDatadir := ctx.String(NewDatadirFlag.Name)
	forkSnapDir := newDatadir + "/snapshots"
	files, bytes, err := dl.WriteForkDatadir(dl.WriteForkDatadirOpts{
		ParentSnapDir:   parentSnapDir,
		ForkSnapDir:     forkSnapDir,
		ForkChainConfig: derived,
		Plan:            plan,
	})
	if err != nil {
		return fmt.Errorf("write fork datadir: %w", err)
	}
	logger.Info("[fork-from] fork datadir written",
		"datadir", newDatadir,
		"files_copied", files,
		"bytes_copied", bytes)
	logger.Info("[fork-from] EL-side complete; pair with CL setup (Phase 2c-CL) before starting erigon")
	return nil
}

// loadChainConfig reads a chain.json file and unmarshals into chain.Config.
func loadChainConfig(path string) (*chain.Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	bytes, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg chain.Config
	if err := unmarshalJSONStrict(bytes, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.ChainName == "" {
		return nil, fmt.Errorf("%s: chainName field is empty — not a valid chain.json", path)
	}
	if cfg.ChainID == nil || cfg.ChainID.Sign() == 0 {
		return nil, fmt.Errorf("%s: chainId field is missing or zero", path)
	}
	return &cfg, nil
}

// unmarshalJSONStrict is a thin wrapper that decodes with
// json.Decoder + DisallowUnknownFields for early errors on
// malformed configs.
func unmarshalJSONStrict(data []byte, v any) error {
	dec := newStrictDecoder(data)
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}
