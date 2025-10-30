package version

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
			"v1.0",
			args{v: "v1.0"},
			V1_0,
			false,
		},
		{
			"v1",
			args{v: "v1"},
			V1_0,
			false,
		},
		{
			"v1.0-008800-008900-bormilestones.seg",
			args{v: "v1.0-008800-008900-bormilestones.seg"},
			V1_0,
			false,
		},
		{
			"v1-008800-008900-bormilestones.seg",
			args{v: "v1-008800-008900-bormilestones.seg"},
			V1_0,
			false,
		},
		{
			"v2.0-008800-008900-bormilestones.seg",
			args{v: "v2.0-008800-008900-bormilestones.seg"},
			V2_0,
			false,
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
