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

package metrics

import (
	"fmt"
	"strings"
	"time"
)

type HistTimer struct {
	Histogram
	start time.Time
	name  string
}

func NewHistTimer(name string) *HistTimer {
	rawName := strings.Split(name, "{")
	return &HistTimer{
		Histogram: GetOrCreateHistogram(name),
		start:     time.Now(),
		name:      rawName[0],
	}
}

func (h *HistTimer) PutSince() {
	h.Histogram.ObserveDuration(h.start)
}

func (h *HistTimer) Tag(pairs ...string) *HistTimer {
	if len(pairs)%2 != 0 {
		pairs = append(pairs, "UNEQUAL_KEY_VALUE_TAGS")
	}

	var toJoin []string
	for i := 0; i < len(pairs); i = i + 2 {
		toJoin = append(toJoin, fmt.Sprintf(`%s="%s"`, pairs[i], pairs[i+1]))
	}

	tags := ""
	if len(toJoin) > 0 {
		tags = "{" + strings.Join(toJoin, ",") + "}"
	}

	return &HistTimer{
		Histogram: GetOrCreateHistogram(h.name + tags),
		start:     time.Now(),
		name:      h.name,
	}
}

func (h *HistTimer) Child(suffix string) *HistTimer {
	suffix = strings.TrimPrefix(suffix, "_")
	return NewHistTimer(h.name + "_" + suffix)
}
