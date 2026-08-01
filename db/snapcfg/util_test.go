package snapcfg

import (
	"testing"

	"github.com/stretchr/testify/require"

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
