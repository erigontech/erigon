package remote

import (
	"strings"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

func NodeInfoReplyLess(i, j *types.NodeInfoReply) bool {
	if cmp := strings.Compare(i.Name, j.Name); cmp != 0 {
		return cmp == -1
	}
	return strings.Compare(i.Enode, j.Enode) == -1
}
