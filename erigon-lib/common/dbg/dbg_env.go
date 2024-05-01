package dbg

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/c2h5oh/datasize"
)

func EnvString(envVarName string, defaultVal string) string {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		fmt.Printf("[dbg] env %s=%s\n", envVarName, v)
		return v
	}
	return defaultVal
}
func EnvBool(envVarName string, defaultVal bool) bool {
	v, _ := os.LookupEnv(envVarName)
	if v == "true" {
		fmt.Printf("[dbg] env %s=%t\n", envVarName, true)
		return true
	}
	if v == "false" {
		fmt.Printf("[dbg] env %s=%t\n", envVarName, false)
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
		fmt.Printf("[dbg] env %s=%d\n", envVarName, i)
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
		fmt.Printf("[dbg] env %s=%s\n", envVarName, val)
		return val
	}
	return defaultVal
}

func EnvDuration(envVarName string, defaultVal time.Duration) time.Duration {
	v, _ := os.LookupEnv(envVarName)
	if v != "" {
		fmt.Printf("[dbg] env %s=%s\n", envVarName, v)

		val, err := time.ParseDuration(v)
		if err != nil {
			panic(err)
		}
		return val
	}
	return defaultVal
}
