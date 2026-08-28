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

package integrity

import (
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func edgeBranchRecord() []byte {
	record := make([]byte, 1+2+32)
	record[0] = 0x10
	binary.BigEndian.PutUint16(record[1:3], 1)
	record[3] = 1
	return record
}

func v3IntegrityFile(t *testing.T) fakeVisibleFile {
	t.Helper()
	path := writeCommitmentRecords(t,
		[]byte{0}, []byte("state-blob"),
		nibbles.ChildKeyV3(nibbles.EncodeKeyV3(nil), 1), edgeBranchRecord(),
	)
	return fakeVisibleFile{
		path:     path,
		endTxNum: 1,
		version:  version.Version{Major: 3, Minor: 0},
	}
}

func requireEdgeRecordRefusal(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, err.Error(), "edge-record")
	require.Contains(t, err.Error(), "unsupported")
}

func TestIntegrityChecksRefuseV3EdgeRecords(t *testing.T) {
	file := v3IntegrityFile(t)
	logger := log.New()

	t.Run("deref", func(t *testing.T) {
		_, err := checkCommitmentKvDeref(t.Context(), file, 1, true, logger)
		requireEdgeRecordRefusal(t, err)
	})
	t.Run("history values", func(t *testing.T) {
		_, err := checkCommitmentHistValBucket(t.Context(), nil, nil, file, 0, true, log.LvlDebug, logger)
		requireEdgeRecordRefusal(t, err)
	})
	t.Run("base correspondence", func(t *testing.T) {
		err := checkStateCorrespondenceBase(t.Context(), file, 1, true, logger)
		requireEdgeRecordRefusal(t, err)
	})
	t.Run("reverse correspondence", func(t *testing.T) {
		err := checkStateCorrespondenceReverse(t.Context(), file, nil, nil, 1, true, logger)
		requireEdgeRecordRefusal(t, err)
	})
	t.Run("next-file references", func(t *testing.T) {
		err := extractCommitmentRefsToCollectors(t.Context(), file, nil, nil, logger)
		requireEdgeRecordRefusal(t, err)
	})
	t.Run("hash verification", func(t *testing.T) {
		err := checkHashVerification(t.Context(), file, 1, true, 1, logger)
		requireEdgeRecordRefusal(t, err)
	})
}

func TestIntegrityRecordChecksRefuseAnEdgeRecordValue(t *testing.T) {
	record := edgeBranchRecord()
	logger := log.New()

	_, _, err := checkDerefBranch(nil, record, nil, nil, nil, nil, "fixture.kv", false, logger)
	requireEdgeRecordRefusal(t, err)

	var hashMismatches, hashChecked atomic.Uint64
	err = verifyHashItem(
		hashWorkItem{branchKey: []byte{1}, branchValue: record},
		true,
		"fixture.kv",
		false,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		&hashMismatches,
		&hashChecked,
		logger,
	)
	requireEdgeRecordRefusal(t, err)
}

func TestCommitmentFileScanSkipsStateKeyWithoutSkippingLegacyRoot(t *testing.T) {
	path := writeCommitmentRecords(t,
		[]byte{0}, accountBranch(make([]byte, 20)),
		[]byte("state"), []byte("state-blob"),
	)
	file := fakeVisibleFile{path: path, endTxNum: 20, version: version.V2_0}

	scan := computeCommitmentFileScan(file)
	require.False(t, scan.referenced)
	require.Equal(t, derefCounts{branchKeys: 1, plainAccounts: 1}, scan.counts)
}

func TestCommitmentStateKeySelectionFollowsFileFormat(t *testing.T) {
	legacy := fakeVisibleFile{version: version.V2_0}
	v3 := fakeVisibleFile{version: version.Version{Major: 3, Minor: 0}}

	require.True(t, isCommitmentStateKeyForFile([]byte("state"), legacy))
	require.False(t, isCommitmentStateKeyForFile([]byte{0}, legacy))
	require.True(t, isCommitmentStateKeyForFile([]byte{0}, v3))
	require.False(t, isCommitmentStateKeyForFile([]byte("state"), v3))
}

func TestCommitmentFileScanMarksEdgeRecordsUnsupported(t *testing.T) {
	scan := computeCommitmentFileScan(v3IntegrityFile(t))
	require.True(t, scan.unsupported)
	require.False(t, scan.referenced)
}
