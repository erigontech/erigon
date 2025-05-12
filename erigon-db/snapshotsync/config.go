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

package snapshotsync

import "strings"

type BlocksFreezing struct {
	KeepBlocks        bool // produce new snapshots of blocks but don't remove blocks from DB
	ProduceE2         bool // produce new block files
	ProduceE3         bool // produce new state files
	NoDownloader      bool // possible to use snapshots without calling Downloader
	Verify            bool // verify snapshots on startup
	DisableDownloadE3 bool // disable download state snapshots
	DownloaderAddr    string
	ChainName         string
}

func (s BlocksFreezing) String() string {
	var out []string
	if s.KeepBlocks {
		out = append(out, "--"+FlagSnapKeepBlocks+"=true")
	}
	if !s.ProduceE2 {
		out = append(out, "--"+FlagSnapStop+"=true")
	}
	return strings.Join(out, " ")
}

var (
	FlagSnapKeepBlocks = "snap.keepblocks"
	FlagSnapStop       = "snap.stop"
	FlagSnapStateStop  = "snap.state.stop"
)

func NewSnapCfg(keepBlocks, produceE2, produceE3 bool, chainName string) BlocksFreezing {
	return BlocksFreezing{KeepBlocks: keepBlocks, ProduceE2: produceE2, ProduceE3: produceE3, ChainName: chainName}
}
