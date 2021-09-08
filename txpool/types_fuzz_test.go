//go:build gofuzzbeta
// +build gofuzzbeta

package txpool

import (
	"testing"
)

// https://blog.golang.org/fuzz-beta
// golang.org/s/draft-fuzzing-design
//gotip doc testing
//gotip doc testing.F
//gotip doc testing.F.AddRemoteTxs
//gotip doc testing.F.Fuzz

// gotip test -trimpath -v -fuzz=Fuzz -fuzztime=10s ./txpool

//func init() {
//	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
//}

func FuzzParseTx(f *testing.F) {
	f.Add([]byte{1}, 0)
	f.Fuzz(func(t *testing.T, in []byte, pos int) {
		t.Parallel()
		ctx := NewTxParseContext()
		txn := &TxSlot{}
		sender := make([]byte, 20)
		_, _ = ctx.ParseTransaction(in, pos, txn, sender)
	})
}
