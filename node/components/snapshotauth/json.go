// Copyright 2026 The Erigon Authors
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

package snapshotauth

import (
	"encoding/hex"
	"encoding/json"
	"time"
)

// jsonView is the human-readable form of a Delegation. Pubkeys and
// signatures render as hex, timestamps as RFC3339 (or "indefinite" for
// the zero-sentinel), and the parent chain is unfolded recursively so
// the whole chain is visible at a glance.
//
// This is READ-ONLY — there is no JSON unmarshaller for Delegation.
// CBOR is the canonical on-disk + on-wire form; JSON exists solely for
// inspection (CLI dump, log diagnostics, debugging).
type jsonView struct {
	Version      uint8     `json:"version"`
	Issuer       string    `json:"issuer"`
	Audience     string    `json:"audience"`
	Capabilities []string  `json:"capabilities"`
	NotBefore    string    `json:"notBefore"`
	Expires      string    `json:"expires"`
	DepthCap     uint16    `json:"depthCap"`
	Parent       *jsonView `json:"parent,omitempty"`
	Signature    string    `json:"signature"`
}

// MarshalJSON renders the delegation as a human-readable JSON
// document. Recursively unfolds the Parent chain so consumers see the
// whole authority path at once. Returns an error only if a nested
// parent fails to decode — encoding itself is infallible.
//
// Output is compact JSON; callers wanting indentation should pass the
// result through json.Indent or call json.MarshalIndent on the
// Delegation directly (Go's json package strips whitespace from
// MarshalJSON output before re-emitting, so indenting here is lost).
func (d *Delegation) MarshalJSON() ([]byte, error) {
	view, err := d.toJSONView()
	if err != nil {
		return nil, err
	}
	return json.Marshal(view)
}

func (d *Delegation) toJSONView() (*jsonView, error) {
	v := &jsonView{
		Version:      d.Version,
		Issuer:       hex.EncodeToString(d.Issuer),
		Audience:     hex.EncodeToString(d.Audience),
		Capabilities: append([]string(nil), d.Capabilities...),
		NotBefore:    formatTimestamp(d.NotBefore),
		Expires:      formatExpiry(d.Expires),
		DepthCap:     d.DepthCap,
		Signature:    hex.EncodeToString(d.Signature),
	}
	if len(d.Parent) > 0 {
		parent, err := Decode(d.Parent)
		if err != nil {
			return nil, err
		}
		pv, err := parent.toJSONView()
		if err != nil {
			return nil, err
		}
		v.Parent = pv
	}
	return v, nil
}

func formatTimestamp(unix int64) string {
	if unix == 0 {
		return "immediately"
	}
	return time.Unix(unix, 0).UTC().Format(time.RFC3339)
}

func formatExpiry(unix int64) string {
	if unix == 0 {
		return "indefinite"
	}
	return time.Unix(unix, 0).UTC().Format(time.RFC3339)
}
