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

package app

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/node/components/snapshotauth"
	"github.com/erigontech/erigon/p2p/enode"
)

var (
	delegateTargetENRFlag = cli.StringFlag{
		Name:     "target-enr",
		Usage:    "ENR of the node to delegate to (literal string starting with 'enr:' or path to a file containing one)",
		Required: true,
	}
	delegateSignerKeyFlag = cli.StringFlag{
		Name:     "signer-key",
		Usage:    "Path to the operator's secp256k1 private key in hex form (one line)",
		Required: true,
	}
	delegateCapabilitiesFlag = cli.StringFlag{
		Name:     "capabilities",
		Usage:    "Comma-separated capability list (e.g. 'snapshot:advertise,snapshot:serve,snapshot:delegate')",
		Required: true,
	}
	delegateExpiresFlag = cli.StringFlag{
		Name:  "expires",
		Usage: "Expiry as RFC3339 timestamp (e.g. 2027-01-01T00:00:00Z) or 'never' for indefinite",
		Value: "never",
	}
	delegateDepthFlag = cli.UintFlag{
		Name:  "depth",
		Usage: "Maximum further-delegation depth allowed by this attestation (0 = leaf, no further delegation)",
		Value: 0,
	}
	delegateOutputFlag = cli.StringFlag{
		Name:     "output",
		Usage:    "Path to write the canonical CBOR delegation",
		Required: true,
	}
	delegateParentFlag = cli.StringFlag{
		Name:  "parent",
		Usage: "Optional path to a parent delegation CBOR; the new delegation chains from it",
	}
	delegateJSONFlag = cli.BoolFlag{
		Name:  "print-json",
		Usage: "Print the delegation as JSON to stdout (read-only inspection view)",
	}
)

// doSnapshotDelegate is the action body for `erigon snapshot delegate`.
// Steps:
//  1. Load operator private key from --signer-key (hex).
//  2. Parse target ENR; extract the audience secp256k1 pubkey.
//  3. Optionally load and decode a parent delegation (chained issuance).
//  4. Build + sign a Delegation per the flags.
//  5. Write canonical CBOR to --output.
//  6. If --print-json is set, render the JSON view to stdout.
func doSnapshotDelegate(cliCtx *cli.Context) error {
	signerKey, err := crypto.LoadECDSA(delegateSignerKeyFlag.Get(cliCtx))
	if err != nil {
		return fmt.Errorf("loading signer key from %q: %w", delegateSignerKeyFlag.Get(cliCtx), err)
	}

	targetEnr, err := readENR(delegateTargetENRFlag.Get(cliCtx))
	if err != nil {
		return fmt.Errorf("reading target ENR: %w", err)
	}
	targetNode, err := enode.Parse(enode.ValidSchemes, targetEnr)
	if err != nil {
		return fmt.Errorf("parsing target ENR: %w", err)
	}
	audiencePub := targetNode.Pubkey()
	if audiencePub == nil {
		return fmt.Errorf("target ENR has no secp256k1 public key")
	}

	caps, err := snapshotauth.ParseCapabilities(delegateCapabilitiesFlag.Get(cliCtx))
	if err != nil {
		return err
	}

	var notBefore, expires time.Time
	expiresFlag := strings.TrimSpace(delegateExpiresFlag.Get(cliCtx))
	if !strings.EqualFold(expiresFlag, "never") {
		expires, err = time.Parse(time.RFC3339, expiresFlag)
		if err != nil {
			return fmt.Errorf("parsing --expires %q (must be RFC3339 or 'never'): %w", expiresFlag, err)
		}
	}

	var parentBytes []byte
	if path := delegateParentFlag.Get(cliCtx); path != "" {
		parentBytes, err = os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading parent delegation: %w", err)
		}
		// Sanity-decode so we surface a malformed parent up front.
		if _, err := snapshotauth.Decode(parentBytes); err != nil {
			return fmt.Errorf("parent delegation is not a valid snapshotauth artefact: %w", err)
		}
	}

	depthCap := uint16(cliCtx.Uint(delegateDepthFlag.Name))

	d, err := snapshotauth.New(
		&signerKey.PublicKey,
		audiencePub,
		caps,
		notBefore,
		expires,
		depthCap,
		parentBytes,
	)
	if err != nil {
		return fmt.Errorf("constructing delegation: %w", err)
	}
	if err := d.Sign(signerKey); err != nil {
		return fmt.Errorf("signing delegation: %w", err)
	}

	encoded, err := d.Encode()
	if err != nil {
		return fmt.Errorf("encoding delegation: %w", err)
	}
	if err := os.WriteFile(delegateOutputFlag.Get(cliCtx), encoded, 0o600); err != nil {
		return fmt.Errorf("writing delegation to %q: %w", delegateOutputFlag.Get(cliCtx), err)
	}

	fmt.Fprintf(cliCtx.App.Writer, "wrote delegation (%d bytes) to %s\n",
		len(encoded), delegateOutputFlag.Get(cliCtx))

	if cliCtx.Bool(delegateJSONFlag.Name) {
		js, err := d.MarshalJSON()
		if err != nil {
			return fmt.Errorf("rendering JSON view: %w", err)
		}
		fmt.Fprintln(cliCtx.App.Writer, string(js))
	}
	return nil
}

// readENR returns the target-enr flag value as a literal ENR string.
// Accepts either the canonical base64 `enr:...` form or the legacy
// `enode://...` URL form (both supported by enode.Parse). If the value
// matches neither, it is treated as a path to a file whose first
// non-empty, non-comment line is the ENR.
func readENR(input string) (string, error) {
	input = strings.TrimSpace(input)
	if strings.HasPrefix(input, "enr:") || strings.HasPrefix(input, "enode://") {
		return input, nil
	}
	data, err := os.ReadFile(input)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		return line, nil
	}
	return "", fmt.Errorf("file %q contained no ENR", input)
}
