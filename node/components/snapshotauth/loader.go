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

package snapshotauth

import (
	"crypto/ecdsa"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

// DefaultDelegationFileName is the file name a node looks for under its
// datadir when no explicit path is configured. Mirrors the jwt.hex
// convention (`<datadir>/<filename>` with a CLI flag override).
const DefaultDelegationFileName = "snapshot.ucan"

// BootstrapDepthCap is the depth caveat baked into auto-generated
// self-signed bootstrap delegations. A value high enough that operators
// can re-delegate freely without bumping into the cap; a future
// finalization pass may tighten this once delegation chains have a
// settled shape.
const BootstrapDepthCap uint16 = 16

// LoadOrGenerateDelegation returns the operator's snapshot delegation
// bytes (canonical CBOR ready to feed into RollingV2Publisher.
// SetDelegationSource).
//
// Path resolution mirrors ObtainJWTSecret's pattern:
//
//   - If `configuredPath` is set, read that exact file. Missing/corrupt
//     is a hard error — operators who explicitly named a path expect
//     it to exist.
//
//   - If `configuredPath` is empty, fall back to
//     <datadir>/snapshot.ucan. Read it if present. If missing AND
//     `nodeKey` is non-nil, generate a self-signed bootstrap
//     delegation (issuer == audience == nodeKey.PublicKey, all caps,
//     indefinite expiry, BootstrapDepthCap depth) and write it to the
//     default path. Future starts read the cached file.
//
// The self-signed bootstrap is only useful as an attestation when
// remote peers happen to trust THIS node's pubkey directly (e.g. this
// node is a bootnode in their config). For other deployments,
// operators are expected to obtain a real delegation via
// `erigon snapshots delegate` and place it at the default path or
// pass --snapshot.delegation=<path>; the bootstrap simply makes the
// producer side non-failing on first run.
//
// Returns (nil, nil) when no delegation is configured AND no nodeKey
// is supplied — caller treats this as "publish V2 without UCAN
// sidecar" (consumers running with TrustConfig will reject this
// peer's manifests, which is the correct behaviour for a node that
// hasn't opted into the trust system).
func LoadOrGenerateDelegation(configuredPath, datadir string, nodeKey *ecdsa.PrivateKey, logger log.Logger) ([]byte, error) {
	if logger == nil {
		logger = log.Root()
	}

	if configuredPath != "" {
		data, err := os.ReadFile(configuredPath)
		if err != nil {
			return nil, fmt.Errorf("reading snapshot delegation %s: %w", configuredPath, err)
		}
		if _, derr := Decode(data); derr != nil {
			return nil, fmt.Errorf("decoding snapshot delegation %s: %w", configuredPath, derr)
		}
		logger.Info("[snapshotauth] loaded delegation", "path", configuredPath)
		return data, nil
	}

	if datadir == "" {
		// No explicit path AND no datadir to anchor a default: caller
		// is running ad-hoc (a CLI tool, a test harness without a
		// data directory). Treat as "no delegation" — let the caller
		// decide whether that's an error.
		return nil, nil
	}

	defaultPath := filepath.Join(datadir, DefaultDelegationFileName)
	if data, err := os.ReadFile(defaultPath); err == nil {
		if _, derr := Decode(data); derr != nil {
			return nil, fmt.Errorf("decoding snapshot delegation %s: %w", defaultPath, derr)
		}
		logger.Info("[snapshotauth] loaded delegation", "path", defaultPath)
		return data, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading snapshot delegation %s: %w", defaultPath, err)
	}

	if nodeKey == nil {
		logger.Info("[snapshotauth] no delegation configured; publishing V2 without UCAN sidecar",
			"path", defaultPath)
		return nil, nil
	}

	// Generate self-signed bootstrap.
	d, err := New(
		&nodeKey.PublicKey, &nodeKey.PublicKey,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		time.Time{}, time.Time{}, BootstrapDepthCap, nil,
	)
	if err != nil {
		return nil, fmt.Errorf("constructing bootstrap delegation: %w", err)
	}
	if err := d.Sign(nodeKey); err != nil {
		return nil, fmt.Errorf("signing bootstrap delegation: %w", err)
	}
	encoded, err := d.Encode()
	if err != nil {
		return nil, fmt.Errorf("encoding bootstrap delegation: %w", err)
	}
	if err := os.WriteFile(defaultPath, encoded, 0o600); err != nil {
		return nil, fmt.Errorf("writing %s: %w", defaultPath, err)
	}
	logger.Info("[snapshotauth] generated self-signed bootstrap delegation",
		"path", defaultPath)
	return encoded, nil
}
