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
	Enabled               bool
	KeyPath               string
	BidMargin             float64
	MaxBidMargin          float64
	MinProfitGwei         uint64
	BidDelay              time.Duration
	BidPublishLead        time.Duration
	CollateralWarningGwei uint64
	MaxPending            int
	MaxRetained           int
	RetryInterval         time.Duration
}

func DefaultConfig() Config {
	return Config{
		BidMargin:             0.95,
		MaxBidMargin:          0.97,
		BidPublishLead:        400 * time.Millisecond,
		CollateralWarningGwei: 20_000_000_000,
		MaxRetained:           16,
		RetryInterval:         250 * time.Millisecond,
	}
}
