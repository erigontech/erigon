//go:build darwin || linux

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

package state_test

import (
	"bytes"
	"sort"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	commitmenttemporal "github.com/erigontech/erigon/internal/commitmenttest/temporal"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestPBinSixRangesFit256Descriptors(t *testing.T) {
	selectPBinRangeWriterHash(t)
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 56)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 6, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	writer, err := state.NewPBinRangeWriterWithLimitsForTest(agg, kv.CommitmentBinDomain, 47, 7, 1<<30)
	require.NoError(t, err)
	leaves := make([]state.PBinLeaf, 0, 3*16*6*2)
	for _, zone := range []byte{eip8297.AccountZone, eip8297.CodeZone, eip8297.StorageZone} {
		for nibble := range byte(16) {
			for stampRange := range byte(6) {
				for branch := range byte(2) {
					size := eip8297.AccountKeyLength
					value := pbinRangeWriterBasicValue(1)
					if zone == eip8297.CodeZone {
						value = pbinRangeWriterCodeValue(1)
					}
					if zone == eip8297.StorageZone {
						size = eip8297.StorageKeyLength
						value = pbinRangeWriterValue(1)
					}
					key := make([]byte, size)
					key[0], key[1], key[2], key[3] = zone, nibble<<4, stampRange, branch
					leaves = append(leaves, state.PBinLeaf{Key: key, Value: value, Stamp: uint64(stampRange) * 8})
				}
			}
		}
	}
	sort.Slice(leaves, func(i, j int) bool { return bytes.Compare(leaves[i].Key, leaves[j].Key) < 0 })
	var previous unix.Rlimit
	require.NoError(t, unix.Getrlimit(unix.RLIMIT_NOFILE, &previous))
	limit := previous
	limit.Cur = 256
	require.NoError(t, unix.Setrlimit(unix.RLIMIT_NOFILE, &limit))
	defer func() { require.NoError(t, unix.Setrlimit(unix.RLIMIT_NOFILE, &previous)) }()
	_, err = writer.WriteAtBlock(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		for _, leaf := range leaves {
			if emitErr := emit(leaf); emitErr != nil {
				return emitErr
			}
		}
		return nil
	}, 0)
	require.NoError(t, err, "six ranges with 48 partitions must convert with RLIMIT_NOFILE=256")
}
