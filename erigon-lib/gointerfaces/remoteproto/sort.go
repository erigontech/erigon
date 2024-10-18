// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package remoteproto

import (
	"strings"

	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
)

func NodeInfoReplyCmp(i, j *types.NodeInfoReply) int {
	if cmp := strings.Compare(i.Name, j.Name); cmp != 0 {
		return cmp
	}
	return strings.Compare(i.Enode, j.Enode)
}
