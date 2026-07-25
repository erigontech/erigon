// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package btindex

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

// The pivot offsets are stored Elias-Fano-encoded (nodeOfftEF). Decoding and
// lookups must be correct for both the v0 (legacy list) and v2 (footer) layouts.
func Test_BtreeIndex_NodeOfftEF_V0_V2(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	const M = uint64(32)
	keyCount := 500
	compressFlags := seg.CompressKeys | seg.CompressVals

	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, compressFlags)
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	truth := map[string][]byte{}
	d, err := seg.NewDecompressor(dataPath)
	require.NoError(t, err)
	gt := seg.NewReader(d.MakeGetter(), compressFlags)
	gt.Reset(0)
	for gt.HasNext() {
		k, _ := gt.Next(nil)
		v, _ := gt.Next(nil)
		truth[string(k)] = common.Copy(v)
	}
	d.Close()

	v2Path := filepath.Join(tmp, "v2.bt")
	buildBtreeIndex(t, dataPath, v2Path, compressFlags, 1, logger, true)
	v0Path := filepath.Join(tmp, "v0.bt")
	writeV0Index(t, dataPath, v0Path, compressFlags, M)

	for _, tc := range []struct{ name, path string }{{"v0", v0Path}, {"v2", v2Path}} {
		t.Run(tc.name, func(t *testing.T) {
			kv, bt, err := OpenBtreeIndexAndDataFile(tc.path, dataPath, M, compressFlags, false)
			require.NoError(t, err)
			defer bt.Close()
			defer kv.Close()
			require.NotNil(t, bt.bplus.nodeOfftEF, "pivot offsets must be Elias-Fano-encoded")

			gr := seg.NewReader(kv.MakeGetter(), compressFlags)
			for i := range keys {
				_, v, _, found, err := bt.Get(keys[i], gr)
				require.NoErrorf(t, err, "i=%d", i)
				require.Truef(t, found, "key %d not found", i)
				require.Equalf(t, truth[string(keys[i])], v, "key %d value mismatch", i)
			}
		})
	}
}
