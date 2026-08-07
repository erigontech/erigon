package snapcfg

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"

	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

func typedNames(t *testing.T, items PreverifiedItems) []string {
	t.Helper()
	typed := Preverified{Items: items}.Typed(snaptype.CaplinSnapshotTypes)
	names := make([]string, 0, len(typed.Items))
	for _, item := range typed.Items {
		names = append(names, item.Name)
	}
	return names
}

func TestTypedCaplinVersionWindow(t *testing.T) {
	names := typedNames(t, PreverifiedItems{
		{Name: "caplin/v0.5-000000-000100-BlockRoot.seg", Hash: "below-min"},
		{Name: "caplin/v1.1-000000-000100-BlockRoot.seg", Hash: "in-window"},
		{Name: "caplin/v9.9-000000-000100-BlockRoot.seg", Hash: "above-preferred"},
	})
	require.Equal(t, []string{"caplin/v1.1-000000-000100-BlockRoot.seg"}, names)
}

func TestTypedCaplinKeepsNewestPerName(t *testing.T) {
	names := typedNames(t, PreverifiedItems{
		{Name: "caplin/v1.0-000000-000100-BlockRoot.seg", Hash: "old"},
		{Name: "caplin/v1.0-000100-000200-BlockRoot.seg", Hash: "other-range"},
		{Name: "caplin/v1.1-000000-000100-BlockRoot.seg", Hash: "new"},
	})
	require.Equal(t, []string{
		"caplin/v1.0-000100-000200-BlockRoot.seg",
		"caplin/v1.1-000000-000100-BlockRoot.seg",
	}, names)
}

// Accessors under caplin/ share the data-type version window, unlike the typed path
// which picks the index version.
func TestTypedCaplinAppliesWindowToIndexes(t *testing.T) {
	names := typedNames(t, PreverifiedItems{
		{Name: "caplin/v0.5-000000-000100-BlockRoot.idx", Hash: "below-min"},
		{Name: "caplin/v1.1-000000-000100-BlockRoot.idx", Hash: "in-window"},
		{Name: "caplin/v9.9-000000-000100-BlockRoot.idx", Hash: "above-preferred"},
	})
	require.Equal(t, []string{"caplin/v1.1-000000-000100-BlockRoot.idx"}, names)
}

func TestTypedCaplinDropsUnparseableVersion(t *testing.T) {
	names := typedNames(t, PreverifiedItems{
		{Name: "caplin/salt-blocks.txt", Hash: "no-version"},
		{Name: "caplin/v1.1-000000-000100-BlockRoot.seg", Hash: "in-window"},
	})
	require.Equal(t, []string{"caplin/v1.1-000000-000100-BlockRoot.seg"}, names)
}

func TestDroppedNamesReportsUnparseableCaplinEntry(t *testing.T) {
	items := PreverifiedItems{
		{Name: "caplin/salt-blocks.txt", Hash: "no-version"},
		{Name: "caplin/v1.1-000000-000100-BlockRoot.seg", Hash: "in-window"},
	}
	typed := Preverified{Items: items}.Typed(snaptype.CaplinSnapshotTypes)

	names, total := droppedNames(items, typed.Items, maxLoggedDroppedNames)
	require.Equal(t, 1, total)
	require.Equal(t, []string{"caplin/salt-blocks.txt"}, names)
}

func TestDroppedNamesCapsEnumeration(t *testing.T) {
	items := PreverifiedItems{
		{Name: "caplin/bad1-000000-000100-BlockRoot.seg"},
		{Name: "caplin/bad2-000000-000100-BlockRoot.seg"},
		{Name: "caplin/bad3-000000-000100-BlockRoot.seg"},
		{Name: "caplin/v1.1-000000-000100-BlockRoot.seg"},
	}
	typed := Preverified{Items: items}.Typed(snaptype.CaplinSnapshotTypes)

	names, total := droppedNames(items, typed.Items, 2)
	require.Equal(t, 3, total)
	require.Equal(t, []string{
		"caplin/bad1-000000-000100-BlockRoot.seg",
		"caplin/bad2-000000-000100-BlockRoot.seg",
	}, names)
}

// keepNewest is shared by the caplin and the generic branch, and it compares the
// versions it was given rather than re-deriving them from the stored item's name.
func TestKeepNewestPrefersHigherVersion(t *testing.T) {
	var best btree.Map[string, PreverifiedItem]
	var kept btree.Map[string, snaptype.Version]
	older := PreverifiedItem{Name: "v1.0-000000-000100-headers.seg", Hash: "old"}
	newer := PreverifiedItem{Name: "v1.1-000000-000100-headers.seg", Hash: "new"}

	keepNewest(&best, &kept, "000000-000100-headers.seg", older, version.V1_0)
	keepNewest(&best, &kept, "000000-000100-headers.seg", newer, version.V1_1)
	got, ok := best.Get("000000-000100-headers.seg")
	require.True(t, ok)
	require.Equal(t, newer, got)

	keepNewest(&best, &kept, "000000-000100-headers.seg", older, version.V1_0)
	got, _ = best.Get("000000-000100-headers.seg")
	require.Equal(t, newer, got, "an older version must not displace the one already stored")
}

func TestTypedKeepsCaplinTypedEntries(t *testing.T) {
	items := PreverifiedItems{
		{Name: "v1.1-000000-000100-beaconblocks.idx", Hash: "a"},
		{Name: "v1.1-000000-000100-beaconblocks.seg", Hash: "b"},
		{Name: "v1.1-000000-000100-blobsidecars.idx", Hash: "c"},
		{Name: "v1.1-000000-000100-blobsidecars.seg", Hash: "d"},
	}
	typed := Preverified{Items: items}.Typed(snaptype.CaplinSnapshotTypes)
	require.Equal(t, items, typed.Items)
}

func TestNameToParts(t *testing.T) {
	type args struct {
		name string
		v    snaptype.Version
	}
	tests := []struct {
		name      string
		args      args
		wantBlock uint64
		wantErr   bool
	}{
		{
			"happy pass",
			args{
				name: "v1.0-asd-12-d",
				v:    version.ZeroVersion,
			},
			12,
			false,
		},
		{
			"happy pass with version",
			args{
				name: "v2.0-asd-12-d",
				v:    version.V2_0,
			},
			12,
			false,
		},
		{
			"happy pass && block in the end",
			args{
				name: "v1.0-asd-12",
				v:    version.ZeroVersion,
			},
			12,
			false,
		},
		{
			"version mismatch",
			args{
				name: "v1.0-asd-12",
				v:    version.V2_0,
			},
			0,
			true,
		},
		{
			"block parse error",
			args{
				name: "v1.0-asd-dd12",
				v:    version.ZeroVersion,
			},
			0,
			true,
		},
		{
			"bad name",
			args{
				name: "v1.0-dd12",
				v:    version.ZeroVersion,
			},
			0,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBlock, err := ExtractBlockFromName(tt.args.name, tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractBlockFromName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotBlock != tt.wantBlock {
				t.Errorf("ExtractBlockFromName() gotBlock = %v, want %v", gotBlock, tt.wantBlock)
			}
		})
	}
}
