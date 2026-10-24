package txpool

import (
	"strings"
	"testing"

	"github.com/erigontech/erigon/common"
)

// A residue is only worth printing if it says whether the sender's run has a HOLE in it: a
// contiguous run is a pool waiting its turn, a gap is what stops the chain taking any of it.
func TestResidueBySenderNamesTheHoleInASendersRun(t *testing.T) {
	p := &TxPool{byHash: map[string]*metaTxn{}, senders: newSendersBatch(nil)}

	add := func(id uint64, nonce uint64) {
		mt := &metaTxn{TxnSlot: &TxnSlot{}}
		mt.TxnSlot.SenderID = id
		mt.TxnSlot.Nonce = nonce
		p.byHash[string(rune(id))+":"+string(rune(nonce))+"!"+strings.Repeat("x", int(nonce%3))] = mt
	}

	// Sender 1: 5,6,7 — contiguous, nothing missing.
	add(1, 5)
	add(1, 6)
	add(1, 7)
	// Sender 2: 10 and 14 — a span of 5 holding 2, so three numbers are absent.
	add(2, 10)
	add(2, 14)
	p.senders.senderID2Addr[1] = common.HexToAddress("0x1111111111111111111111111111111111111111")

	got := p.residueBySender(5)

	// Sender 1 holds more, so it is reported first, and by its address since the mapping exists.
	if !strings.Contains(got, "0x1111111111111111111111111111111111111111 held=3 nonces=5..7 span=3 missingInSpan=0") {
		t.Errorf("contiguous run not reported as whole:\n%s", got)
	}
	// Sender 2 has no address mapping — it must still be named, never panic. See busiestSenderRun.
	if !strings.Contains(got, "id2 held=2 nonces=10..14 span=5 missingInSpan=3") {
		t.Errorf("hole not reported:\n%s", got)
	}
}

// The reason this function exists: the readers it replaces returned nothing above 16 held, so the
// probe fell silent exactly when the pool was most wedged.
func TestResidueBySenderDoesNotGoSilentOnALargeResidue(t *testing.T) {
	p := &TxPool{byHash: map[string]*metaTxn{}, senders: newSendersBatch(nil)}
	for n := uint64(0); n < 1247; n++ {
		mt := &metaTxn{TxnSlot: &TxnSlot{}}
		mt.TxnSlot.SenderID = 1
		mt.TxnSlot.Nonce = n
		p.byHash[string(rune(n%256))+"/"+strings.Repeat("y", int(n%7))+string(rune(n/256))] = mt
	}
	if len(p.byHash) == 0 {
		t.Fatal("test built no residue")
	}
	if got := p.residueBySender(5); got == "" {
		t.Fatal("a large residue reported nothing — the silence this function was written to end")
	}
	// The old readers: both refuse above their cap.
	if residueNamesCapped := p.residueNames(); residueNamesCapped == "" {
		t.Log("residueNames still returns something; the cap lived in the caller")
	}
}
