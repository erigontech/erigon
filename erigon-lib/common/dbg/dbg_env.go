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
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/log/v3"
)

const ErigonEnvPrefix = "ERIGON_"

// envLookup - auto-add ERIGON_ prefix to any declared ENV variable
//
//	User - can add/skip ERIGON_ prefix
//	Developer - can add/skip ERIGON_ prefix
func envLookup(envVarName string) (string, bool) {
	if v, ok := os.LookupEnv(envVarName); ok {
		if strings.HasPrefix(envVarName, ErigonEnvPrefix) {
			log.Warn("[env]", envVarName, v)
		} else {
			log.Warn("[env] use ERIGON_ prefix for env", "var", envVarName)
			log.Warn("[env]", envVarName, v)
		}
		return v, true
	}
	if v, ok := os.LookupEnv(ErigonEnvPrefix + envVarName); ok {
		log.Warn("[env]", ErigonEnvPrefix+envVarName, v)
		return v, true
	}
	return "", false
}

func EnvString(envVarName string, defaultVal string) string {
	if v, _ := envLookup(envVarName); v != "" {
		return v
	}
	return defaultVal
}

func EnvStrings(envVarName string, sep string, defaultVal []string) []string {
	if v, _ := envLookup(envVarName); v != "" {
		return strings.Split(v, sep)
	}
	return defaultVal
}

func EnvBool(envVarName string, defaultVal bool) bool {
	v, _ := envLookup(envVarName)
	if strings.EqualFold(v, "true") {
		return true
	}
	if strings.EqualFold(v, "false") {
		return false
	}
	return defaultVal
}

func EnvInt(envVarName string, defaultVal int) int {
	if v, _ := envLookup(envVarName); v != "" {
		return int(MustParseInt(v))
	}
	return defaultVal
}

func EnvUint(envVarName string, defaultVal uint64) uint64 {
	if v, _ := envLookup(envVarName); v != "" {
		return MustParseUint(v)
	}
	return defaultVal
}

func EnvInts(envVarName string, sep string, defaultVal []int64) []int64 {
	if v, _ := envLookup(envVarName); v != "" {
		return MustParseInts(v, sep)
	}
	return defaultVal
}

func EnvUints(envVarName string, sep string, defaultVal []uint64) []uint64 {
	if v, _ := envLookup(envVarName); v != "" {
		return MustParseUints(v, sep)
	}
	return defaultVal
}

func EnvDataSize(envVarName string, defaultVal datasize.ByteSize) datasize.ByteSize {
	if v, _ := envLookup(envVarName); v != "" {
		val, err := datasize.ParseString(v)
		if err != nil {
			panic(err)
		}
		return val
	}
	return defaultVal
}

func EnvDuration(envVarName string, defaultVal time.Duration) time.Duration {
	if v, _ := envLookup(envVarName); v != "" {
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
func MustParseInts(strNum, separator string) []int64 {
	if strings.EqualFold(strNum, "all") || strings.EqualFold(strNum, "true") {
		return []int64{math.MaxInt64}
	}
	parts := strings.Split(strNum, separator)
	ints := make([]int64, 0, len(parts))
	for _, str := range parts {
		ints = append(ints, MustParseInt(str))
	}
	return ints
}
func MustParseUints(strNum, separator string) []uint64 {
	if strings.EqualFold(strNum, "all") || strings.EqualFold(strNum, "true") {
		return []uint64{math.MaxUint64}
	}
	parts := strings.Split(strNum, separator)
	ints := make([]uint64, 0, len(parts))
	for _, str := range strings.Split(strNum, separator) {
		ints = append(ints, MustParseUint(str))
	}
	return ints
}
