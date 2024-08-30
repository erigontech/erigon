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

package config3

// AggregationStep number of transactions in smallest static file
const HistoryV3AggregationStep = 1_562_500 // = 100M / 64. Dividers: 2, 5, 10, 20, 50, 100, 500
//const HistoryV3AggregationStep = 1_562_500 / 10

const EnableHistoryV4InTest = true

const MaxReorgDepthV3 = 1024
