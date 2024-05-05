package remoteproto

import (
	"strings"

	types "github.com/ledgerwatch/erigon-lib/gointerfaces/typesproto"
)

func NodeInfoReplyCmp(i, j *types.NodeInfoReply) int {
	if cmp := strings.Compare(i.Name, j.Name); cmp != 0 {
		return cmp
	}
	return strings.Compare(i.Enode, j.Enode)
}
