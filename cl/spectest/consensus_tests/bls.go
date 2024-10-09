package consensus_tests

import (
	"github.com/ledgerwatch/erigon/spectest"
	"io/fs"
	"testing"
)

type BlsAggregateVerify struct {
}

func (b *BlsAggregateVerify) Run(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	t.Skipf("Skipping BLS because it is done by library")
	return
	//var meta struct {
	//	Input struct {
	//		Pubkeys   []hexutility.Bytes `yaml:"pubkeys"`
	//		Messages  []common.Hash      `yaml:"messages"`
	//		Signature hexutility.Bytes   `yaml:"signature"`
	//	} `yaml:"input"`
	//	Output bool `yaml:"output"`
	//}
	//if err := spectest.ReadMeta(root, "data.yaml", &meta); err != nil {
	//	return err
	//}
	//return spectest.ErrorHandlerNotImplemented
}
