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

package dbg

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/log/v3"
)

func envLookup(envVarName string) (string, bool) {
	if v, ok := os.LookupEnv(envVarName); ok {
		if !strings.HasPrefix(envVarName, "ERIGON_") {
			log.Warn("[env] please use ERIGON_ prefix for env variables of erigon", "var", envVarName)
		}
		log.Warn("[env]", envVarName, v)
		return v, true
	}
	if v, ok := os.LookupEnv("ERIGON_" + envVarName); ok {
		log.Warn("[env]", envVarName, v)
		return v, true
	}
	return "", false
}

func EnvString(envVarName string, defaultVal string) string {
	v, _ := envLookup(envVarName)
	if v != "" {
		return v
	}
	return defaultVal
}

func EnvStrings(envVarName string, sep string, defaultVal []string) []string {
	v, _ := envLookup(envVarName)
	if v != "" {
		return strings.Split(v, sep)
	}
	return defaultVal
}

func EnvBool(envVarName string, defaultVal bool) bool {
	v, _ := envLookup(envVarName)
	if strings.ToLower(v) == "true" {
		return true
	}
	if strings.ToLower(v) == "false" {
		return false
	}
	return defaultVal
}

func EnvInt(envVarName string, defaultVal int) int {
	v, _ := envLookup(envVarName)
	if v != "" {
		return int(MustParseInt(v))
	}
	return defaultVal
}

func EnvUint(envVarName string, defaultVal uint64) uint64 {
	v, _ := envLookup(envVarName)
	if v != "" {
		return MustParseUint(v)
	}
	return defaultVal
}

func EnvInts(envVarName string, sep string, defaultVal []int64) []int64 {
	v, _ := envLookup(envVarName)
	if v != "" {
		var ints []int64
		for _, str := range strings.Split(v, sep) {
			ints = append(ints, MustParseInt(str))
		}
		return ints
	}
	return defaultVal
}

func EnvUints(envVarName string, sep string, defaultVal []uint64) []uint64 {
	v, _ := envLookup(envVarName)
	if v != "" {
		var ints []uint64
		for _, str := range strings.Split(v, sep) {
			ints = append(ints, MustParseUint(str))
		}
		return ints
	}
	return defaultVal
}

func EnvDataSize(envVarName string, defaultVal datasize.ByteSize) datasize.ByteSize {
	v, _ := envLookup(envVarName)
	if v != "" {
		val, err := datasize.ParseString(v)
		if err != nil {
			panic(err)
		}
		return val
	}
	return defaultVal
}

func EnvDuration(envVarName string, defaultVal time.Duration) time.Duration {
	v, _ := envLookup(envVarName)
	if v != "" {
		val, err := time.ParseDuration(v)
		if err != nil {
			panic(err)
		}
		return val
	}
	return defaultVal
}

func MustParseInt(strNum string) int64 {
	cleanNum := strings.ReplaceAll(strNum, "_", "")
	parsed, err := strconv.ParseInt(cleanNum, 10, 64)
	if err != nil {
		panic(fmt.Errorf("%w, str: %s", err, strNum))
	}
	return parsed
}

func MustParseUint(strNum string) uint64 {
	cleanNum := strings.ReplaceAll(strNum, "_", "")
	parsed, err := strconv.ParseUint(cleanNum, 10, 64)
	if err != nil {
		panic(fmt.Errorf("%w, str: %s", err, strNum))
	}
	return parsed
}
