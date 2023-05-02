package consensus_tests

import (
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/cmd/ef-tests-cl/consensus_tests/spectest"
)

type BlsAggregateVerify struct {
}

func (b *BlsAggregateVerify) Run(t *testing.T, root fs.FS) (passed bool, err error) {
	var meta struct {
		Input struct {
			Pubkeys   []hexutility.Bytes `yaml:"pubkeys"`
			Messages  []common.Hash      `yaml:"messages"`
			Signature hexutility.Bytes   `yaml:"signature"`
		} `yaml:"input"`
		Output bool `yaml:"output"`
	}
	if err := spectest.ReadMeta(root, "data.yaml", &meta); err != nil {
		return false, err
	}
	return false, spectest.ErrorHandlerNotImplemented
}
