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
	"bytes"
	"crypto/elliptic"
	"encoding/hex"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/node/components/snapshotauth"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func TestDoSnapshotDelegate_RootSignAndDecode(t *testing.T) {
	dir := t.TempDir()

	// Operator signing key.
	signerKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	signerPath := filepath.Join(dir, "signer.key")
	require.NoError(t, crypto.SaveECDSA(signerPath, signerKey))

	// Target node ENR.
	targetKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	var rec enr.Record
	require.NoError(t, enode.SignV4(&rec, targetKey))
	targetNode, err := enode.New(enode.ValidSchemes, &rec)
	require.NoError(t, err)
	enrStr := targetNode.URLv4()

	outPath := filepath.Join(dir, "delegation.cbor")

	c := newDelegateCLI(t, []string{
		"--target-enr", enrStr,
		"--signer-key", signerPath,
		"--capabilities", "snapshot:advertise,snapshot:serve",
		"--expires", "never",
		"--depth", "1",
		"--output", outPath,
	})

	require.NoError(t, doSnapshotDelegate(c))

	raw, err := os.ReadFile(outPath)
	require.NoError(t, err)
	d, err := snapshotauth.Decode(raw)
	require.NoError(t, err)
	require.NoError(t, d.VerifySignature())

	// Audience pubkey in the delegation must match the compressed
	// form of the target ENR's pubkey.
	wantAudience := elliptic.MarshalCompressed(targetKey.PublicKey.Curve, targetKey.PublicKey.X, targetKey.PublicKey.Y)
	require.Equal(t, wantAudience, d.Audience)

	// Issuer matches operator pubkey.
	wantIssuer := elliptic.MarshalCompressed(signerKey.PublicKey.Curve, signerKey.PublicKey.X, signerKey.PublicKey.Y)
	require.Equal(t, wantIssuer, d.Issuer)

	require.Equal(t, []string{"snapshot:advertise", "snapshot:serve"}, d.Capabilities)
	require.Equal(t, uint16(1), d.DepthCap)
	require.Equal(t, int64(0), d.Expires, "indefinite expiry stored as zero sentinel")
	require.Empty(t, d.Parent, "root delegation has no parent")
}

func TestDoSnapshotDelegate_ChainsFromParent(t *testing.T) {
	dir := t.TempDir()

	// Root operator signs a delegation to mid.
	rootKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	rootPath := filepath.Join(dir, "root.key")
	require.NoError(t, crypto.SaveECDSA(rootPath, rootKey))

	midKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	midPath := filepath.Join(dir, "mid.key")
	require.NoError(t, crypto.SaveECDSA(midPath, midKey))

	// Mid ENR (so root can delegate to it).
	var midRec enr.Record
	require.NoError(t, enode.SignV4(&midRec, midKey))
	midNode, err := enode.New(enode.ValidSchemes, &midRec)
	require.NoError(t, err)
	midEnr := midNode.URLv4()

	rootOut := filepath.Join(dir, "root.cbor")
	require.NoError(t, doSnapshotDelegate(newDelegateCLI(t, []string{
		"--target-enr", midEnr,
		"--signer-key", rootPath,
		"--capabilities", "snapshot:advertise,snapshot:delegate",
		"--depth", "2",
		"--output", rootOut,
	})))

	// Leaf ENR.
	leafKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	var leafRec enr.Record
	require.NoError(t, enode.SignV4(&leafRec, leafKey))
	leafNode, err := enode.New(enode.ValidSchemes, &leafRec)
	require.NoError(t, err)
	leafEnr := leafNode.URLv4()

	// Mid signs a chained delegation to leaf.
	leafOut := filepath.Join(dir, "leaf.cbor")
	require.NoError(t, doSnapshotDelegate(newDelegateCLI(t, []string{
		"--target-enr", leafEnr,
		"--signer-key", midPath,
		"--capabilities", "snapshot:advertise",
		"--depth", "1",
		"--output", leafOut,
		"--parent", rootOut,
	})))

	raw, err := os.ReadFile(leafOut)
	require.NoError(t, err)
	leaf, err := snapshotauth.Decode(raw)
	require.NoError(t, err)
	require.NoError(t, leaf.VerifySignature())
	require.NotEmpty(t, leaf.Parent)

	parent, err := snapshotauth.Decode(leaf.Parent)
	require.NoError(t, err)
	require.NoError(t, parent.VerifySignature())
	require.Equal(t, parent.Audience, leaf.Issuer,
		"chain integrity: parent.audience == child.issuer")
}

func TestDoSnapshotDelegate_RejectsBadCapability(t *testing.T) {
	dir := t.TempDir()

	signerKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	signerPath := filepath.Join(dir, "signer.key")
	require.NoError(t, crypto.SaveECDSA(signerPath, signerKey))

	targetKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	var rec enr.Record
	require.NoError(t, enode.SignV4(&rec, targetKey))
	targetNode, err := enode.New(enode.ValidSchemes, &rec)
	require.NoError(t, err)

	c := newDelegateCLI(t, []string{
		"--target-enr", targetNode.URLv4(),
		"--signer-key", signerPath,
		"--capabilities", "snapshot:typo",
		"--output", filepath.Join(dir, "out.cbor"),
	})
	err = doSnapshotDelegate(c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown capability")
}

func TestReadENR_FromFile(t *testing.T) {
	dir := t.TempDir()
	targetKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	var rec enr.Record
	require.NoError(t, enode.SignV4(&rec, targetKey))
	targetNode, err := enode.New(enode.ValidSchemes, &rec)
	require.NoError(t, err)

	p := filepath.Join(dir, "node.enr")
	require.NoError(t, os.WriteFile(p, []byte("# leading comment\n\n"+targetNode.URLv4()+"\n"), 0o600))
	got, err := readENR(p)
	require.NoError(t, err)
	require.Equal(t, targetNode.URLv4(), got,
		"first non-empty non-comment line is the ENR")
}

// newDelegateCLI builds a *cli.Context with the given flags pre-set so
// the action body runs as if invoked from the command line.
func newDelegateCLI(t *testing.T, args []string) *cli.Context {
	t.Helper()
	app := &cli.App{
		Writer:    &bytes.Buffer{},
		ErrWriter: &bytes.Buffer{},
	}
	set := flag.NewFlagSet("delegate", flag.ContinueOnError)
	flags := []cli.Flag{
		&delegateTargetENRFlag,
		&delegateSignerKeyFlag,
		&delegateCapabilitiesFlag,
		&delegateExpiresFlag,
		&delegateDepthFlag,
		&delegateOutputFlag,
		&delegateParentFlag,
		&delegateJSONFlag,
	}
	for _, f := range flags {
		require.NoError(t, f.Apply(set))
	}
	require.NoError(t, set.Parse(args))
	return cli.NewContext(app, set, nil)
}

// silence unused-import warning for hex in case future tests need it.
var _ = hex.EncodeToString
