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
package chain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// ErrUnknownL2 reports an l2 section whose name has no registered decoder.
var ErrUnknownL2 = errors.New("no L2 config registered")

// L2ConfigDecoder decodes an l2 section into the stack's L2Config.
type L2ConfigDecoder func(raw json.RawMessage) (L2Config, error)

var (
	l2DecodersMu sync.RWMutex
	l2Decoders   = map[string]L2ConfigDecoder{}
)

// RegisterL2Config registers the decoder of the l2 sections whose "name"
// member is name. Panics on an empty name, a nil decoder or a duplicate.
func RegisterL2Config(name string, decode L2ConfigDecoder) {
	if name == "" {
		panic("chain: RegisterL2Config: empty name")
	}
	if decode == nil {
		panic("chain: RegisterL2Config: nil decoder for " + name)
	}
	l2DecodersMu.Lock()
	defer l2DecodersMu.Unlock()
	if _, exists := l2Decoders[name]; exists {
		panic("chain: L2 config already registered: " + name)
	}
	l2Decoders[name] = decode
}

func unregisterL2Config(name string) {
	l2DecodersMu.Lock()
	defer l2DecodersMu.Unlock()
	delete(l2Decoders, name)
}

func l2Decoder(name string) (L2ConfigDecoder, bool) {
	l2DecodersMu.RLock()
	defer l2DecodersMu.RUnlock()
	decode, ok := l2Decoders[name]
	return decode, ok
}

// ResolveL2 sets L2 from L2JSON with the decoder registered under the
// section's name. It does nothing when L2 is already set or there is no l2
// section, and returns ErrUnknownL2 when no decoder is registered.
func (c *Config) ResolveL2() error {
	if c == nil || c.L2 != nil || len(c.L2JSON) == 0 || bytes.Equal(c.L2JSON, jsonNull) {
		return nil
	}
	var probe struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(c.L2JSON, &probe); err != nil {
		return fmt.Errorf("chain config l2: %w", err)
	}
	decode, ok := l2Decoder(probe.Name)
	if !ok {
		return fmt.Errorf("%w: chain config l2 name %q", ErrUnknownL2, probe.Name)
	}
	l2, err := decode(c.L2JSON)
	if err != nil {
		return fmt.Errorf("chain config l2 %q: %w", probe.Name, err)
	}
	c.L2 = l2
	return nil
}
