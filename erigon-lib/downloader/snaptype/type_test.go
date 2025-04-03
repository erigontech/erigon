package snaptype

import (
	"reflect"
	"testing"
)

func TestParseVersion(t *testing.T) {
	type args struct {
		v string
	}
	tests := []struct {
		name    string
		args    args
		want    Version
		wantErr bool
	}{
		{
			name: "new file versions format",
			args: args{
				"v1.0",
			},
			want:    V1_0,
			wantErr: false,
		},
		{
			name: "old file versions format",
			args: args{
				"v1",
			},
			want:    V1_0,
			wantErr: false,
		},
		{
			name: "wrong file versions format",
			args: args{
				"v-1-0",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVersion(tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseVersion() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsOldFilename(t *testing.T) {
	type args struct {
		filename string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "new file versions format",
			args: args{
				"v1.0-accounts.192-256.vi",
			},
			want: false,
		},
		{
			name: "old file versions format",
			args: args{
				"v1-accounts.192-256.vi",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsOldFilename(tt.args.filename); got != tt.want {
				t.Errorf("IsOldFilename() = %v, want %v", got, tt.want)
			}
		})
	}
}
