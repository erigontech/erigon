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

package validation

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

func TestProducer_NilProducerAcceptsAll(t *testing.T) {
	var p *Producer
	require.NoError(t, p.Validate(&snapshot.FileEntry{Name: "x"}, nil))
}

func TestProducer_DefaultProducerRunsBuiltinsOnly(t *testing.T) {
	p := NewDefaultProducer()
	require.NotNil(t, p)
	require.NotEmpty(t, p.Chain)
	require.Empty(t, p.Plugins)

	good := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-1024.kv", Domain: "accounts",
		FromStep: 0, ToStep: 1024, Kind: snapshot.KindKV,
	}
	require.NoError(t, p.Validate(good, nil))
}

func TestProducer_BuiltinFailureReportedAsBuiltin(t *testing.T) {
	p := &Producer{
		Chain:   Chain{NameNotEmpty{}},
		Plugins: Chain{},
	}
	err := p.Validate(&snapshot.FileEntry{Name: ""}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "producer built-in")
	require.Contains(t, err.Error(), "name_not_empty")
}

func TestProducer_PluginFailureReportedAsPlugin(t *testing.T) {
	rejectingPlugin := &stubValidator{name: "deployment_reject", err: errors.New("workflow signoff missing")}
	p := &Producer{
		Chain:   Chain{},
		Plugins: Chain{rejectingPlugin},
	}
	err := p.Validate(&snapshot.FileEntry{Name: "x"}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "producer plugin")
	require.Contains(t, err.Error(), "deployment_reject")
	require.Contains(t, err.Error(), "workflow signoff missing")
	require.Equal(t, 1, rejectingPlugin.calls)
}

func TestProducer_BuiltinShortCircuitsBeforePlugin(t *testing.T) {
	plugin := &stubValidator{name: "p"}
	p := &Producer{
		Chain:   Chain{NameNotEmpty{}},
		Plugins: Chain{plugin},
	}
	err := p.Validate(&snapshot.FileEntry{Name: ""}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "producer built-in")
	require.Equal(t, 0, plugin.calls,
		"plugin chain must not run when built-in chain rejects")
}

func TestProducer_MarkAdvertisableHappyPath(t *testing.T) {
	inv := snapshot.NewInventory()
	file := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-1024.kv", Domain: "accounts",
		FromStep: 0, ToStep: 1024, Kind: snapshot.KindKV,
		Local: true,
	}
	inv.AddFile(file)
	require.False(t, file.Advertisable, "advertisable starts false")

	p := NewDefaultProducer()
	changed, err := p.MarkAdvertisable(inv, file, nil)
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, file.Advertisable, "validation pass flips the flag")
}

func TestProducer_MarkAdvertisableValidationFailureLeavesFlagFalse(t *testing.T) {
	inv := snapshot.NewInventory()
	file := &snapshot.FileEntry{
		Name:     "", // will fail NameNotEmpty
		FromStep: 0, ToStep: 1024,
	}
	inv.AddFile(file)

	p := NewDefaultProducer()
	changed, err := p.MarkAdvertisable(inv, file, nil)
	require.Error(t, err)
	require.False(t, changed)
	require.False(t, file.Advertisable,
		"validation failure must leave Advertisable=false")
}

func TestProducer_MarkAdvertisableIdempotent(t *testing.T) {
	inv := snapshot.NewInventory()
	file := &snapshot.FileEntry{
		Name: "v1.0-accounts.0-1024.kv", Domain: "accounts",
		FromStep: 0, ToStep: 1024, Kind: snapshot.KindKV,
	}
	inv.AddFile(file)

	p := NewDefaultProducer()
	first, err := p.MarkAdvertisable(inv, file, nil)
	require.NoError(t, err)
	require.True(t, first, "first call flips the flag")

	second, err := p.MarkAdvertisable(inv, file, nil)
	require.NoError(t, err)
	require.False(t, second,
		"second call returns false (already advertisable) but no error")
}

func TestInventory_MarkAdvertisableUnknownFile(t *testing.T) {
	inv := snapshot.NewInventory()
	require.False(t, inv.MarkAdvertisable("nonexistent.kv"),
		"flagging an unknown file returns false (no panic)")
}

func TestInventory_MarkAdvertisableAcrossKinds(t *testing.T) {
	inv := snapshot.NewInventory()

	files := []*snapshot.FileEntry{
		{Name: "v1.0-accounts.0-1024.kv", Domain: "accounts", FromStep: 0, ToStep: 1024, Kind: snapshot.KindKV},
		{Name: "v1.0-000000-000500-headers.seg", FromStep: 0, ToStep: 500, Kind: snapshot.KindKV},
		{Name: "erigondb.toml", Kind: snapshot.KindMeta},
		{Name: "salt-blocks.txt", Kind: snapshot.KindSalt},
		{Name: "caplin/v1.0-beaconblocks-0.seg", Kind: snapshot.KindCaplin},
	}
	for _, f := range files {
		inv.AddFile(f)
	}

	for _, f := range files {
		require.True(t, inv.MarkAdvertisable(f.Name),
			"first MarkAdvertisable for %s must flip the flag", f.Name)
		require.True(t, f.Advertisable,
			"%s.Advertisable must now be true", f.Name)
	}
}
