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

package witness

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

type pbinBlobVector struct {
	Name     string         `json:"name"`
	Kind     string         `json:"kind"`
	Position int            `json:"position"`
	Stem     string         `json:"stem"`
	Leaves   []pbinBlobLeaf `json:"leaves"`
	Prefix   string         `json:"prefix"`
	Left     string         `json:"left"`
	Right    string         `json:"right"`
	Key      string         `json:"key"`
	Value    string         `json:"value"`
	Blob     string         `json:"blob"`
	Hash     string         `json:"hash"`
}

type pbinBlobLeaf struct {
	Sub   byte   `json:"sub"`
	Value string `json:"value"`
}

func pbinBlobVectors(t *testing.T) []pbinBlobVector {
	t.Helper()
	data, err := os.ReadFile("testdata/geth_blobs.json")
	require.NoError(t, err)
	var vectors []pbinBlobVector
	require.NoError(t, json.Unmarshal(data, &vectors))
	return vectors
}

func TestPBinBlobVectorsHash(t *testing.T) {
	pbinUseBlake3(t)

	for _, vector := range pbinBlobVectors(t) {
		blob, err := hex.DecodeString(vector.Blob[2:])
		require.NoError(t, err, vector.Name)
		_, err = PBinDecodeBlob(blob)
		require.NoError(t, err, vector.Name)
		got, err := PBinHashBlob(blob)
		require.NoError(t, err, vector.Name)
		require.Equal(t, vector.Hash, "0x"+hex.EncodeToString(got[:]), "hash comparison for %s", vector.Name)
	}
}

func TestPBinBlobVectorsRoundTrip(t *testing.T) {
	pbinUseBlake3(t)

	for _, vector := range pbinBlobVectors(t) {
		blob := pbinDecodeHex(t, vector.Blob)
		decoded, err := PBinDecodeBlob(blob)
		require.NoError(t, err, vector.Name)
		var roundTrip []byte
		switch vector.Kind {
		case "leaf":
			roundTrip, err = PBinEncodeLeaf(pbinDecodeHex(t, vector.Key), pbinDecodeHex(t, vector.Value))
		case "branch":
			roundTrip, err = PBinEncodeBranch(&decoded.Branch.Prefix, &decoded.Branch.Left, &decoded.Branch.Right)
		case "group":
			group := decoded.Group
			roundTrip, err = PBinEncodeGroup(*group)
			require.Equal(t, pbinDecodeHex(t, vector.Stem), group.Stem, vector.Name)
			require.Equal(t, vector.Position, int(group.Position), vector.Name)
			require.Len(t, group.Subs, len(vector.Leaves), vector.Name)
			for i, leaf := range vector.Leaves {
				require.Equal(t, leaf.Sub, group.Subs[i], vector.Name)
				require.Equal(t, pbinDecodeHex(t, leaf.Value), group.Values[i], vector.Name)
			}
		default:
			t.Fatalf("unknown vector kind %q", vector.Kind)
		}
		require.NoError(t, err, vector.Name)
		require.Equal(t, blob, roundTrip, vector.Name)
	}
}

func TestPBinBlobRejectsInvalidRecords(t *testing.T) {
	pbinUseBlake3(t)
	valid := pbinDecodeHex(t, pbinBlobVectors(t)[4].Blob)
	validGroup, err := PBinDecodeBlob(valid)
	require.NoError(t, err)

	tests := []struct {
		name string
		call func() error
	}{
		{name: "group needs two values", call: func() error {
			group := *validGroup.Group
			group.Subs = group.Subs[:1]
			group.Values = group.Values[:1]
			_, err := PBinEncodeGroup(group)
			return err
		}},
		{name: "group value count", call: func() error {
			group := *validGroup.Group
			group.Values = group.Values[:1]
			_, err := PBinEncodeGroup(group)
			return err
		}},
		{name: "group length mismatch", call: func() error {
			_, err := PBinDecodeBlob(valid[:len(valid)-1])
			return err
		}},
		{name: "group position beyond stem", call: func() error {
			group := *validGroup.Group
			group.Position = uint16(len(group.Stem)*8 + 1)
			_, err := PBinEncodeGroup(group)
			return err
		}},
		{name: "group stem length", call: func() error {
			group := *validGroup.Group
			group.Stem = group.Stem[:len(group.Stem)-1]
			_, err := PBinEncodeGroup(group)
			return err
		}},
		{name: "group stem zone", call: func() error {
			group := *validGroup.Group
			group.Stem[0] = 0x02
			_, err := PBinEncodeGroup(group)
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) { require.Error(t, test.call()) })
	}
}

func TestPBinGroupHashMatchesReference(t *testing.T) {
	pbinUseBlake3(t)

	for _, vector := range pbinBlobVectors(t) {
		if vector.Kind != "group" {
			continue
		}
		blob := pbinDecodeHex(t, vector.Blob)
		decoded, err := PBinDecodeBlob(blob)
		require.NoError(t, err, vector.Name)
		want := pbinReferenceGroupHash(decoded.Group)
		got, err := PBinHashBlob(blob)
		require.NoError(t, err, vector.Name)
		require.Equal(t, want, got, vector.Name)
	}
}

func TestPBinPathsUseEncodedOrder(t *testing.T) {
	pbinUseBlake3(t)
	root := eip8297.PathFromBits(nil, 0)
	short := eip8297.PathFromBits([]byte{0x80}, 1)
	long := eip8297.PathFromBits([]byte{0x40}, 3)
	require.Empty(t, PBinPath(&root))
	require.Equal(t, eip8297.AppendBitPrefix(nil, &short), PBinPath(&short))
	paths := [][]byte{PBinPath(&long), PBinPath(&root), PBinPath(&short)}
	slices.SortFunc(paths, bytes.Compare)
	require.True(t, slices.IsSortedFunc(paths, bytes.Compare))
}

func TestPBinWitnessPackageDependencies(t *testing.T) {
	pbinUseBlake3(t)
	root, err := filepath.Abs(filepath.Join(".", "..", "..", "..", ".."))
	require.NoError(t, err)
	cmd := exec.CommandContext(context.Background(), "go", "list", "-deps", "./execution/commitment/eip8297/witness")
	cmd.Dir = root
	out, err := cmd.Output()
	require.NoError(t, err)
	for dep := range strings.SplitSeq(string(out), "\n") {
		require.False(t, strings.HasPrefix(dep, "github.com/erigontech/erigon/execution/commitment/v3/"), dep)
		require.NotEqual(t, "github.com/erigontech/erigon/execution/commitment", dep)
		require.False(t, strings.HasPrefix(dep, "github.com/erigontech/erigon/db/"), dep)
	}
}

func pbinUseBlake3(t *testing.T) {
	t.Helper()
	commitmentflags.Restore(t)
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
}

func pbinDecodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "0x"))
	require.NoError(t, err)
	return decoded
}

func pbinReferenceGroupHash(group *PBinGroup) common.Hash {
	bitsByKey := make([][]byte, len(group.Subs))
	for i, sub := range group.Subs {
		key := append(append([]byte(nil), group.Stem...), sub)
		bitsByKey[i] = eip8297.BitsFromBytes(key)
	}
	var build func(int, int, int) eip8297.Node
	build = func(start, end, depth int) eip8297.Node {
		if end-start == 1 {
			key := append(append([]byte(nil), group.Stem...), group.Subs[start])
			return &eip8297.Leaf{Key: key, Value: group.Values[start]}
		}
		divergence := depth
		for bitsByKey[start][divergence] == bitsByKey[end-1][divergence] {
			divergence++
		}
		middle := start + 1
		for middle < end && bitsByKey[middle][divergence] == 0 {
			middle++
		}
		prefix := append([]byte(nil), bitsByKey[start][depth:divergence]...)
		return &eip8297.Branch{Prefix: prefix, Left: build(start, middle, divergence+1), Right: build(middle, end, divergence+1)}
	}
	return eip8297.MerkelizeWith(build(0, len(group.Subs), int(group.Position)), func(preimage []byte) common.Hash {
		sum := blake3.Sum256(preimage)
		return common.Hash(sum)
	})
}
