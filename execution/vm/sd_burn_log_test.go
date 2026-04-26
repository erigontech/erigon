package vm

import (
	"bytes"
	"strings"
	"testing"
)

func TestClassifySDBurn(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                                         string
		post6780, newContract, selfEqBen, balanceZero bool
		want                                         SDBurnKind
	}{
		// Pre-Cancun: any self-beneficiary with positive balance is a real burn.
		{"pre6780/self/nz", false, false, true, false, SDBurnSelf},
		{"pre6780/self/zero", false, false, true, true, SDBurnNone},
		{"pre6780/other/nz", false, false, false, false, SDBurnNone},

		// Post-Cancun, newContract (contract actually destroyed): same as pre-6780
		// for the self-beneficiary path — balance stays at self and is wiped.
		{"post6780/new/self/nz", true, true, true, false, SDBurnSelf},
		{"post6780/new/self/zero", true, true, true, true, SDBurnNone},
		{"post6780/new/other/nz", true, true, false, false, SDBurnNone},

		// Post-Cancun, !newContract: EIP-6780 blocks destruction. Self-beneficiary
		// with positive balance is the canonical "fake burn" / "burn broken by Cancun".
		{"post6780/exist/self/nz", true, false, true, false, SDBurnFake},
		{"post6780/exist/self/zero", true, false, true, true, SDBurnNone},
		{"post6780/exist/other/nz", true, false, false, false, SDBurnNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ClassifySDBurn(c.post6780, c.newContract, c.selfEqBen, c.balanceZero)
			if got != c.want {
				t.Errorf("ClassifySDBurn(post6780=%v,new=%v,selfEqBen=%v,bZero=%v) = %d, want %d",
					c.post6780, c.newContract, c.selfEqBen, c.balanceZero, got, c.want)
			}
		})
	}
}

func TestSDBurnLogWriter(t *testing.T) {
	var buf bytes.Buffer
	SetSDBurnLogWriter(&buf)

	SDBurnLog("SD-SELF block=%d balance=%s", uint64(19_500_000), "42")
	SDBurnLog("FAKE-BURN block=%d balance=%s", uint64(19_500_001), "0x1")

	out := buf.String()
	for _, want := range []string{
		"SD-SELF block=19500000 balance=42",
		"FAKE-BURN block=19500001 balance=0x1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing log line %q in output:\n%s", want, out)
		}
	}
}
