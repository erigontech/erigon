// Copyright 2025 The Erigon Authors
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

//go:build !linux

package seg

// The async-io residency gate relies on mincore + io_uring and is Linux-only.
// Off Linux these are no-ops so the cross-platform read path still compiles;
// EnableResidencyGate is never called (dbg.FilesAsyncIO stays off), and even if
// it were, ensureResident does nothing.

type residencyBitmap struct{}

func (*residencyBitmap) stop() {}

func (*Getter) EnableResidencyGate() {}

func (*Getter) ensureResident(uint64) {}
