package snapcfg

import (
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"testing"
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
				name: "v1-asd-12-d",
				v:    0,
			},
			12,
			false,
		},
		{
			"happy pass with version",
			args{
				name: "v2-asd-12-d",
				v:    2,
			},
			12,
			false,
		},
		{
			"happy pass && block in the end",
			args{
				name: "v1-asd-12",
				v:    0,
			},
			12,
			false,
		},
		{
			"version mismatch",
			args{
				name: "v1-asd-12",
				v:    2,
			},
			0,
			true,
		},
		{
			"block parse error",
			args{
				name: "v1-asd-dd12",
				v:    0,
			},
			0,
			true,
		},
		{
			"bad name",
			args{
				name: "v1-dd12",
				v:    0,
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
