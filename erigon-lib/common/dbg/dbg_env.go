package dbg

import (
	"os"
	"strconv"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
)

func EnvString(envVarName string, defaultVal string) string {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		log.Info("[dbg] env", envVarName, v)
		return v
	}
	return defaultVal
}
func EnvBool(envVarName string, defaultVal bool) bool {
	v, _ := os.LookupEnv(envVarName)
	if v == "true" {
		log.Info("[dbg] env", envVarName, true)
		return true
	}
	if v == "false" {
		log.Info("[dbg] env", envVarName, false)
		return false
	}
	return defaultVal
}
func EnvInt(envVarName string, defaultVal int) int {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		i, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		log.Info("[dbg] env", envVarName, i)
		return i
	}
	return defaultVal
}
func EnvDataSize(envVarName string, defaultVal datasize.ByteSize) datasize.ByteSize {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		val, err := datasize.ParseString(v)
		if err != nil {
			panic(err)
		}
		log.Info("[dbg] env", envVarName, val)
		return val
	}
	return defaultVal
}

func EnvDuration(envVarName string, defaultVal time.Duration) time.Duration {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		log.Info("[dbg] env", envVarName, v)
		val, err := time.ParseDuration(v)
		if err != nil {
			panic(err)
		}
		return val
	}
	return defaultVal
}
