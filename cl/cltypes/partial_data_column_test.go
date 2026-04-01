package cltypes_test

import (
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

type sszRoot struct {
	Root string `yaml:"root"`
}

func testSSZRoundTrip(t *testing.T, testDir string, version clparams.StateVersion, obj interface {
	DecodeSSZ([]byte, int) error
	EncodeSSZ([]byte) ([]byte, error)
	HashSSZ() ([32]byte, error)
}) {
	t.Helper()
	fsys := os.DirFS(testDir)

	rootBytes, err := fs.ReadFile(fsys, "roots.yaml")
	require.NoError(t, err)

	var root sszRoot
	require.NoError(t, yaml.Unmarshal(rootBytes, &root))
	expectedRoot := common.HexToHash(root.Root)

	snappyEncoded, err := fs.ReadFile(fsys, "serialized.ssz_snappy")
	require.NoError(t, err)

	encoded, err := utils.DecompressSnappy(snappyEncoded, false)
	require.NoError(t, err)

	// Decode
	require.NoError(t, obj.DecodeSSZ(encoded, int(version)))

	// Check hash root
	hashRoot, err := obj.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, hashRoot, "hash root mismatch")

	// Check encode round-trip
	reEncoded, err := obj.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, encoded, reEncoded, "re-encoded bytes mismatch")
}

func TestPartialDataColumnHeader_Fulu(t *testing.T) {
	testDir := "../spectest/tests/mainnet/fulu/ssz_static/PartialDataColumnHeader/ssz_random/case_0"
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("spec test data not found")
	}
	obj := cltypes.NewPartialDataColumnHeader(clparams.FuluVersion)
	testSSZRoundTrip(t, testDir, clparams.FuluVersion, obj)
}

func TestPartialDataColumnHeader_Gloas(t *testing.T) {
	testDir := "../spectest/tests/mainnet/gloas/ssz_static/PartialDataColumnHeader/ssz_random/case_0"
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("spec test data not found")
	}
	obj := cltypes.NewPartialDataColumnHeader(clparams.GloasVersion)
	testSSZRoundTrip(t, testDir, clparams.GloasVersion, obj)
}

func TestPartialDataColumnSidecar_Fulu(t *testing.T) {
	testDir := "../spectest/tests/mainnet/fulu/ssz_static/PartialDataColumnSidecar/ssz_random/case_0"
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("spec test data not found")
	}
	obj := cltypes.NewPartialDataColumnSidecar(clparams.FuluVersion)
	testSSZRoundTrip(t, testDir, clparams.FuluVersion, obj)
}

func TestPartialDataColumnSidecar_Gloas(t *testing.T) {
	testDir := "../spectest/tests/mainnet/gloas/ssz_static/PartialDataColumnSidecar/ssz_random/case_0"
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("spec test data not found")
	}
	obj := cltypes.NewPartialDataColumnSidecar(clparams.GloasVersion)
	testSSZRoundTrip(t, testDir, clparams.GloasVersion, obj)
}

func TestPartialDataColumnPartsMetadata_Fulu(t *testing.T) {
	testDir := "../spectest/tests/mainnet/fulu/ssz_static/PartialDataColumnPartsMetadata/ssz_random/case_0"
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("spec test data not found")
	}
	obj := cltypes.NewPartialDataColumnPartsMetadata()
	testSSZRoundTrip(t, testDir, clparams.FuluVersion, obj)
}

func TestPartialDataColumnPartsMetadata_Gloas(t *testing.T) {
	testDir := "../spectest/tests/mainnet/gloas/ssz_static/PartialDataColumnPartsMetadata/ssz_random/case_0"
	if _, err := os.Stat(testDir); os.IsNotExist(err) {
		t.Skip("spec test data not found")
	}
	obj := cltypes.NewPartialDataColumnPartsMetadata()
	testSSZRoundTrip(t, testDir, clparams.GloasVersion, obj)
}
