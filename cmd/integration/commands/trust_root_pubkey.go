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
	"encoding/hex"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/node/debug"
)

var (
	trustRootPubkeyKeyFile  string
	trustRootPubkeyGenerate bool
)

func init() {
	trustRootPubkeyCmd.Flags().StringVar(&trustRootPubkeyKeyFile, "key", "", "path to a hex-encoded secp256k1 private key file")
	trustRootPubkeyCmd.Flags().BoolVar(&trustRootPubkeyGenerate, "generate-if-missing", false, "if --key does not exist, generate a fresh secp256k1 key and save it to that path (0600)")
	must(trustRootPubkeyCmd.MarkFlagRequired("key"))
	rootCmd.AddCommand(trustRootPubkeyCmd)
}

var trustRootPubkeyCmd = &cobra.Command{
	Use:   "trust_root_pubkey",
	Short: "Print the compressed secp256k1 pubkey (hex) for a trust-root private key",
	Long: `Reads a hex-encoded secp256k1 private key (same format as
crypto.LoadECDSA — one hex line, no 0x prefix) and prints its
33-byte compressed pubkey as 66 hex chars. That form is directly
accepted by --snapshot.trust-roots, so scripts can pipe:

  ROOT_PUB=$(integration trust_root_pubkey --key "$KEY_FILE" --generate-if-missing)
  erigon ... --snapshot.trust-roots="$ROOT_PUB"

Pubkey-only output goes to stdout; logs to stderr.

With --generate-if-missing, an absent --key file triggers a fresh
key generation at that path (0600) instead of failing.`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		if trustRootPubkeyGenerate {
			if _, err := os.Stat(trustRootPubkeyKeyFile); os.IsNotExist(err) {
				k, err := crypto.GenerateKey()
				if err != nil {
					logger.Error("generate key", "err", err)
					os.Exit(1)
				}
				if err := crypto.SaveECDSA(trustRootPubkeyKeyFile, k); err != nil {
					logger.Error("save key", "path", trustRootPubkeyKeyFile, "err", err)
					os.Exit(1)
				}
				if err := os.Chmod(trustRootPubkeyKeyFile, 0o600); err != nil {
					logger.Error("chmod key", "path", trustRootPubkeyKeyFile, "err", err)
					os.Exit(1)
				}
				logger.Info("[trust_root_pubkey] generated fresh key", "path", trustRootPubkeyKeyFile)
			} else if err != nil {
				logger.Error("stat key", "path", trustRootPubkeyKeyFile, "err", err)
				os.Exit(1)
			}
		}
		key, err := crypto.LoadECDSA(trustRootPubkeyKeyFile)
		if err != nil {
			logger.Error("load key", "path", trustRootPubkeyKeyFile, "err", err)
			os.Exit(1)
		}
		pub := crypto.CompressPubkey(&key.PublicKey)
		fmt.Println(hex.EncodeToString(pub))
	},
}
