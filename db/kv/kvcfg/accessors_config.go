// Copyright 2021 The Erigon Authors
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

package kvcfg

import (
	"errors"

	"github.com/erigontech/erigon/db/kv"
)

type ConfigKey []byte

var (
	PersistReceipts   = ConfigKey("persist.receipts")
	CommitmentHistory = ConfigKey("commitment.history")

	// SnapLifecycleDrivenByStorage, SnapP2PManifest, and
	// SnapBootstrapFromPreverified persist the snapshot-flow mode
	// flags so that omitting one on restart can't silently downgrade
	// the datadir to the legacy stage-driven path. Live-rig discovery
	// 2026-06-02: launching without --snap.lifecycle-driven-by-storage
	// left Provider.Inventory nil and the new mode-B snapshot-trim
	// path was a silent no-op.
	SnapLifecycleDrivenByStorage = ConfigKey("snap.lifecycle-driven-by-storage")
	SnapP2PManifest              = ConfigKey("snap.p2p-manifest")
	SnapBootstrapFromPreverified = ConfigKey("snap.bootstrap-from-preverified")

	// SnapTrustFingerprint is a 32-byte sha256 over the canonical
	// encoding of the effective trust-root set (sorted by Pubkey).
	// Locks the trust universe to the datadir: an operator restart
	// pointed at a different trust spec is effectively unauthorised
	// auth rotation, and refuses at startup.
	SnapTrustFingerprint = ConfigKey("snap.trust.fingerprint")
)

func (k ConfigKey) Enabled(tx kv.Tx) (bool, error) { return kv.GetBool(tx, kv.DatabaseInfo, k) }

func (k ConfigKey) EnsureNotChanged(tx kv.RwTx, value bool) (notChanged, enabled bool, err error) {
	return kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, k, value)
}
func (k ConfigKey) ForceWrite(tx kv.RwTx, enabled bool) error {
	if enabled {
		if err := tx.Put(kv.DatabaseInfo, k, []byte{1}); err != nil {
			return err
		}
	} else {
		if err := tx.Put(kv.DatabaseInfo, k, []byte{0}); err != nil {
			return err
		}
	}
	return nil
}
func (k ConfigKey) MustBeEnabled(tx kv.Tx, msg string) error {
	enabled, err := k.Enabled(tx)
	if err != nil {
		return err
	}
	if !enabled {
		return errors.New(msg)
	}
	return nil
}
