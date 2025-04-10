package state

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseStepsFromFileName(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name     string
		args     args
		wantFrom uint64
		wantTo   uint64
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name: "old file format",
			args: args{
				fileName: "v1-accounts.32-40.kv",
			},
			wantFrom: 32,
			wantTo:   40,
			wantErr:  assert.NoError,
		},
		{
			name: "new file format",
			args: args{
				fileName: "v1.0-accounts.32-40.kv",
			},
			wantFrom: 32,
			wantTo:   40,
			wantErr:  assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFrom, gotTo, err := ParseStepsFromFileName(tt.args.fileName)
			if !tt.wantErr(t, err, fmt.Sprintf("ParseStepsFromFileName(%v)", tt.args.fileName)) {
				return
			}
			assert.Equalf(t, tt.wantFrom, gotFrom, "ParseStepsFromFileName(%v)", tt.args.fileName)
			assert.Equalf(t, tt.wantTo, gotTo, "ParseStepsFromFileName(%v)", tt.args.fileName)
		})
	}
}
