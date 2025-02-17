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

func EnvString(envVarName string, defaultVal string) string {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		WarnOnErigonPrefix(envVarName)
		log.Info("[env]", envVarName, v)
		return v
	}

	v, _ = os.LookupEnv("ERIGON_" + envVarName)
	if v != "" {
		log.Info("[env]", envVarName, v)
		return v
	}
	return defaultVal
}
func EnvBool(envVarName string, defaultVal bool) bool {
	v, _ := os.LookupEnv(envVarName)
	if v == "true" {
		WarnOnErigonPrefix(envVarName)
		log.Info("[env]", envVarName, true)
		return true
	}
	if v == "false" {
		WarnOnErigonPrefix(envVarName)
		log.Info("[env]", envVarName, false)
		return false
	}

	v, _ = os.LookupEnv("ERIGON_" + envVarName)
	if v == "true" {
		log.Info("[env]", envVarName, true)
		return true
	}
	if v == "false" {
		log.Info("[env]", envVarName, false)
		return false
	}
	return defaultVal
}
func EnvInt(envVarName string, defaultVal int) int {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		WarnOnErigonPrefix(envVarName)
		i := MustParseInt(v)
		log.Info("[env]", envVarName, i)
		return i
	}

	v, _ = os.LookupEnv("ERIGON_" + envVarName)
	if v != "" {
		i := MustParseInt(v)
		log.Info("[env]", envVarName, i)
		return i
	}
	return defaultVal
}
func EnvDataSize(envVarName string, defaultVal datasize.ByteSize) datasize.ByteSize {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		WarnOnErigonPrefix(envVarName)
		val, err := datasize.ParseString(v)
		if err != nil {
			panic(err)
		}
		log.Info("[env]", envVarName, val)
		return val
	}

	v, _ = os.LookupEnv("ERIGON_" + envVarName)
	if v != "" {
		val, err := datasize.ParseString(v)
		if err != nil {
			panic(err)
		}
		log.Info("[env]", envVarName, val)
		return val
	}
	return defaultVal
}

func EnvDuration(envVarName string, defaultVal time.Duration) time.Duration {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		WarnOnErigonPrefix(envVarName)
		log.Info("[env]", envVarName, v)
		val, err := time.ParseDuration(v)
		if err != nil {
			panic(err)
		}
		return val
	}
	v, _ = os.LookupEnv("ERIGON_" + envVarName)
	if v != "" {
		log.Info("[env]", envVarName, v)
		val, err := time.ParseDuration(v)
		if err != nil {
			panic(err)
		}
		return val
	}
	return defaultVal
}

func WarnOnErigonPrefix(envVarName string) {
	if !strings.HasPrefix(envVarName, "ERIGON_") {
		log.Warn("[env] please use ERIGON_ prefix for env variables of erigon", "var", envVarName)
	}
}

func MustParseInt(strNum string) int {
	cleanNum := strings.ReplaceAll(strNum, "_", "")
	parsed, err := strconv.ParseInt(cleanNum, 10, 64)
	if err != nil {
		panic(fmt.Errorf("%w, str: %s", err, strNum))
	}
	return int(parsed)
}
