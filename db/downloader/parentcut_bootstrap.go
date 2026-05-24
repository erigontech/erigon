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

package downloader

import (
	"fmt"

	"github.com/erigontech/erigon/execution/chain"
)

// IsForkConfig reports whether cfg describes a derived (shadow-fork)
// chain — i.e. carries a non-empty Parent name. Used by startup paths
// to decide whether to emit ForkBootstrapRequired.
//
// nil-safe: returns false for nil cfg.
func IsForkConfig(cfg *chain.Config) bool {
	return cfg != nil && cfg.Parent != ""
}

// ForkBootstrapPlan is the minimal extract from a fork chain.Config
// that bootstrap consumers (manifest_exchange, downloader request
// router) need. Built once at startup from the loaded chain.Config;
// passed to the bus event ForkBootstrapRequired.
type ForkBootstrapPlan struct {
	// Parent is the parent chain name.
	Parent string

	// ParentManifestHash is the 20-byte info-hash of the parent's V2
	// manifest at fork creation. Zero-hash signals "no parent
	// manifest available" — legitimate when the parent was a
	// pre-Phase-1 root chain that doesn't publish V2 manifests; the
	// fork-follower falls back to direct file downloads with no
	// parent-manifest verification.
	ParentManifestHash [20]byte

	// CutBlock is the EL block at which the fork diverges. Used by
	// the request router to classify per-file requests pre-cut vs
	// post-cut.
	CutBlock uint64
}

// BuildForkBootstrapPlan extracts the bootstrap plan from a fork
// chain.Config. Returns nil + nil when cfg is not a fork config
// (IsForkConfig returns false) — callers treat nil as "no bootstrap
// required; root-chain path".
//
// Errors when the chain.Config is a fork (Parent != "") but CutBlock
// is zero — a malformed fork config.
func BuildForkBootstrapPlan(cfg *chain.Config) (*ForkBootstrapPlan, error) {
	if !IsForkConfig(cfg) {
		return nil, nil
	}
	if cfg.CutBlock == 0 {
		return nil, fmt.Errorf("fork chain.Config has Parent=%q but CutBlock=0 — malformed config", cfg.Parent)
	}
	return &ForkBootstrapPlan{
		Parent:             cfg.Parent,
		ParentManifestHash: cfg.ParentManifestHash,
		CutBlock:           cfg.CutBlock,
	}, nil
}
