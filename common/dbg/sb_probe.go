package dbg

import "os"

var (
	SBNoHist   = os.Getenv("SB_NOHIST") == "1"
	SBNoUniq   = os.Getenv("SB_NOUNIQ") == "1"
	SBNoTrie   = os.Getenv("SB_NOTRIE") == "1"
	SBNoCommit = os.Getenv("SB_NOCOMMIT") == "1"
)
