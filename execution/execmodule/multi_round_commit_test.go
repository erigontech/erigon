package execmodule_test

// Multi-round blocks, which is where a commit used to stop being a faithful checkpoint.
//
// A block built over several rounds writes some keys more than once — the coinbase every round pays,
// and any sender spending in more than one round. Those writes used to reach the DB through a stack
// of per-round writers whose txNum ranges OVERLAPPED (the batch kept writing to its own writer across
// a merge: block-start, then a round, then block-end), and only the latest value per (key, step)
// survives in the values table. The writer applied last won, which was not always the newest write,
// so a two-round block committed the coinbase as of the block-start txNum while the batch held the
// round's. Single-round blocks were never affected.
//
// These scenarios do NOT fail on that defect by themselves — the producer accepts its own sealed
// blocks without re-executing them, so a short DB never reaches a root check. Run them with
// ERIGON_WRITER_KEY_PROBE=true and -v, and the [checkpoint] assertion in the FCU commit path reports
// "COMMIT LOST WRITES" for every commit whose keys did not survive. That is the regression guard.
import "testing"

func TestTwoRoundBlockCommitsBothRounds(t *testing.T) {
	checkScenario(t, [][]roundSpec{{{0, 0}, {0, 1}}})
}

// The two rounds spend from DIFFERENT senders, so the only key both write is the coinbase — the
// minimal case, and the one that shows this is not a wholesale drop of the second round.
func TestTwoRoundBlockDifferentSendersCommitsBoth(t *testing.T) {
	checkScenario(t, [][]roundSpec{{{0, 0}, {1, 0}}})
}

func TestThreeRoundBlockCommitsEveryRound(t *testing.T) {
	checkScenario(t, [][]roundSpec{{{0, 0}, {0, 1}, {0, 2}}})
}
