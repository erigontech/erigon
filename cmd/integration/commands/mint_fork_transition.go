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

package commands

import (
	"encoding/base64"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/node/components/snapshotauth"
	"github.com/erigontech/erigon/node/debug"
)

var (
	mintTransitionKeyFile     string
	mintTransitionTargetChain string
	mintTransitionValidity    time.Duration
	mintTransitionOutFile     string
)

func init() {
	mintForkTransitionCmd.Flags().StringVar(&mintTransitionKeyFile, "trust-root-key", "", "path to a hex-encoded secp256k1 private key that is one of the target node's --snapshot.trust-roots")
	mintForkTransitionCmd.Flags().StringVar(&mintTransitionTargetChain, "chain", "", "target fork chain name (must exactly match the debug_setFork target)")
	mintForkTransitionCmd.Flags().DurationVar(&mintTransitionValidity, "validity", snapshotauth.ForkTransitionUCANValidity, "how long the UCAN remains valid")
	mintForkTransitionCmd.Flags().StringVar(&mintTransitionOutFile, "out", "", "write the base64 UCAN to this file (default: stdout)")
	must(mintForkTransitionCmd.MarkFlagRequired("trust-root-key"))
	must(mintForkTransitionCmd.MarkFlagRequired("chain"))
	rootCmd.AddCommand(mintForkTransitionCmd)
}

var mintForkTransitionCmd = &cobra.Command{
	Use:   "mint_fork_transition",
	Short: "Mint a fork-transition UCAN authorising a debug_setFork call",
	Long: `Produces a base64-encoded CBOR UCAN carrying
fork:transition:<chain>, signed by the supplied trust-root key.
Feed the result to 'integration set_fork --authority-ucan-file'
(or --authority-ucan) to authorise a debug_setFork transition.

Phase 1 constraints: the UCAN is self-issued (issuer == audience)
and root-signed (no delegation cascade). The target node verifies
against its --snapshot.trust-roots set; the signing key must
correspond to one of those roots.`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")

		key, err := crypto.LoadECDSA(mintTransitionKeyFile)
		if err != nil {
			logger.Error("load trust-root key", "path", mintTransitionKeyFile, "err", err)
			os.Exit(1)
		}

		now := time.Now()
		enc, err := snapshotauth.MintForkTransitionUCAN(
			key, mintTransitionTargetChain, now, now.Add(mintTransitionValidity),
		)
		if err != nil {
			logger.Error("mint fork-transition UCAN", "chain", mintTransitionTargetChain, "err", err)
			os.Exit(1)
		}
		b64 := base64.StdEncoding.EncodeToString(enc)

		if mintTransitionOutFile == "" {
			fmt.Println(b64)
			return
		}
		if err := os.WriteFile(mintTransitionOutFile, []byte(b64+"\n"), 0o600); err != nil {
			logger.Error("write UCAN file", "path", mintTransitionOutFile, "err", err)
			os.Exit(1)
		}
		logger.Info("[mint_fork_transition] wrote UCAN",
			"chain", mintTransitionTargetChain,
			"valid_until", now.Add(mintTransitionValidity).Format(time.RFC3339),
			"path", mintTransitionOutFile,
		)
	},
}
