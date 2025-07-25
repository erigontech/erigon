// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package params

import (
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/version"
)

var (
	// Following vars are injected through the build flags (see Makefile)
	GitCommit string
	GitBranch string
	GitTag    string
)

const (
	VersionKeyCreated  = "ErigonVersionCreated"
	VersionKeyFinished = "ErigonVersionFinished"
	ClientName         = "erigon"
	ClientCode         = "EG"
)

// Version holds the textual version string.
var Version = func() string {
	return fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Micro)
}()

// VersionWithMeta holds the textual version string including the metadata.
var VersionWithMeta = func() string {
	v := Version
	if version.Modifier != "" {
		v += "-" + version.Modifier
	}
	return v
}()

func VersionWithCommit(gitCommit string) string {
	vsn := VersionWithMeta
	if len(gitCommit) >= 8 {
		vsn += "-" + gitCommit[:8]
	}
	return vsn
}

func SetErigonVersion(tx kv.RwTx, versionKey string) error {
	versionKeyByte := []byte(versionKey)
	hasVersion, err := tx.Has(kv.DatabaseInfo, versionKeyByte)
	if err != nil {
		return err
	}
	if hasVersion {
		return nil
	}
	// Save version if it does not exist
	if err := tx.Put(kv.DatabaseInfo, versionKeyByte, []byte(Version)); err != nil {
		return err
	}
	return nil
}
