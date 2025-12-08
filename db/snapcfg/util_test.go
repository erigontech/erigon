package snapcfg

import (
	"testing"

	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

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
