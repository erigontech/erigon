package remote

import (
	"strings"
)

func (x *NodesInfoReply) Len() int {
	return len(x.NodesInfo)
}

func (x *NodesInfoReply) Less(i, j int) bool {
	if cmp := strings.Compare(x.NodesInfo[i].Name, x.NodesInfo[j].Name); cmp != 0 {
		return cmp == -1
	}
	return strings.Compare(x.NodesInfo[i].Enode, x.NodesInfo[j].Enode) == -1
}

func (x *NodesInfoReply) Swap(i, j int) {
	x.NodesInfo[i], x.NodesInfo[j] = x.NodesInfo[j], x.NodesInfo[i]
}
