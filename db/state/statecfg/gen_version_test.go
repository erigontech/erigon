package statecfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_goStruct(t *testing.T) {
	type args struct {
		dom string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"simple",
			args{"truth"},
			"TruthDomain",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, goStruct(tt.args.dom), "goStruct(%v)", tt.args.dom)
		})
	}
}
