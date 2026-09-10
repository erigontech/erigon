// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbscfg

import "time"

type Config struct {
	Enabled       bool
	KeyPath       string
	BidMargin     float64
	MaxPending    int
	MaxRetained   int
	RetryInterval time.Duration
}

func DefaultConfig() Config {
	return Config{
		BidMargin:     0.85,
		MaxPending:    16,
		MaxRetained:   16,
		RetryInterval: 250 * time.Millisecond,
	}
}
