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

package logging

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/urfave/cli/v2"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/metrics"
)

// Determine the log dir path based on the given urfave context
func LogDirPath(ctx *cli.Context) string {
	dirPath := ""
	if !ctx.Bool(LogDirDisableFlag.Name) {
		dirPath = ctx.String(LogDirPathFlag.Name)
		if dirPath == "" {
			datadir := ctx.String("datadir")
			if datadir != "" {
				dirPath = filepath.Join(datadir, "logs")
			}
		}
	}
	return dirPath
}

// SetupLoggerCtx performs the logging setup according to the parameters
// contained in the given urfave context. It returns either root logger,
// if rootHandler argument is set to true, or a newly created logger.
// This is to ensure gradual transition to the use of non-root logger throughout
// the erigon code without a huge change at once.
// This function which is used in Erigon itself.
// Note: urfave and cobra are two CLI frameworks/libraries for the same functionalities
// and it would make sense to choose one over another
func SetupLoggerCtx(
	filePrefix string,
	ctx *cli.Context,
	consoleDefaultLevel log.Lvl,
	dirDefaultLevel log.Lvl,
	rootHandler bool,
) log.Logger {
	var consoleJson = ctx.Bool(LogJsonFlag.Name) || ctx.Bool(LogConsoleJsonFlag.Name)
	var dirJson = ctx.Bool(LogDirJsonFlag.Name)

	metrics.DelayLoggingEnabled = ctx.Bool(LogBlockDelayFlag.Name)

	consoleLevel := consoleDefaultLevel

	// Priority: LogConsoleVerbosityFlag (if explicitly set) > LogVerbosityFlag (if explicitly set) > default
	if ctx.IsSet(LogConsoleVerbosityFlag.Name) {
		if level, err := tryGetLogLevel(ctx.String(LogConsoleVerbosityFlag.Name)); err == nil {
			consoleLevel = level
		}
	} else if ctx.IsSet(LogVerbosityFlag.Name) {
		if level, err := tryGetLogLevel(ctx.String(LogVerbosityFlag.Name)); err == nil {
			consoleLevel = level
		}
	}

	dirLevel, dErr := tryGetLogLevel(ctx.String(LogDirVerbosityFlag.Name))
	if dErr != nil {
		dirLevel = dirDefaultLevel
	}

	dirPath := ""
	if !ctx.Bool(LogDirDisableFlag.Name) && dirPath != "/dev/null" {
		dirPath = ctx.String(LogDirPathFlag.Name)
		if dirPath == "" {
			datadir := ctx.String("datadir")
			if datadir != "" {
				dirPath = filepath.Join(datadir, "logs")
			}
		}
		if logDirPrefix := ctx.String(LogDirPrefixFlag.Name); len(logDirPrefix) > 0 {
			filePrefix = logDirPrefix
		}
	}

	var logger log.Logger
	if rootHandler {
		logger = log.Root()
	} else {
		logger = log.New()
	}

	initSeparatedLogging(logger, filePrefix, dirPath, consoleLevel, dirLevel, consoleJson, dirJson)
	return logger
}

// SetupLoggerCmd perform the logging for a cobra command, and sets it to the root logger
// This is the function which is NOT used by Erigon itself, but instead by some cobra-based commands,
// for example, rpcdaemon or integration.
// Note: urfave and cobra are two CLI frameworks/libraries for the same functionalities
// and it would make sense to choose one over another
func SetupLoggerCmd(filePrefix string, cmd *cobra.Command) log.Logger {

	logJsonVal, ljerr := cmd.Flags().GetBool(LogJsonFlag.Name)
	if ljerr != nil {
		logJsonVal = false
	}

	logConsoleJsonVal, lcjerr := cmd.Flags().GetBool(LogConsoleJsonFlag.Name)
	if lcjerr != nil {
		logConsoleJsonVal = false
	}

	var consoleJson = logJsonVal || logConsoleJsonVal
	dirJson, djerr := cmd.Flags().GetBool(LogDirJsonFlag.Name)
	if djerr != nil {
		dirJson = false
	}

	consoleLevel, lErr := tryGetLogLevel(cmd.Flags().Lookup(LogConsoleVerbosityFlag.Name).Value.String())
	if lErr != nil {
		// try verbosity flag
		consoleLevel, lErr = tryGetLogLevel(cmd.Flags().Lookup(LogVerbosityFlag.Name).Value.String())
		if lErr != nil {
			consoleLevel = log.LvlInfo
		}
	}

	dirLevel, dErr := tryGetLogLevel(cmd.Flags().Lookup(LogDirVerbosityFlag.Name).Value.String())
	if dErr != nil {
		dirLevel = log.LvlInfo
	}

	dirPath := ""
	disableFileLogging, err := cmd.Flags().GetBool(LogDirDisableFlag.Name)
	if err != nil {
		disableFileLogging = false
	}
	if !disableFileLogging && dirPath != "/dev/null" {
		dirPath = cmd.Flags().Lookup(LogDirPathFlag.Name).Value.String()
		if dirPath == "" {
			datadirFlag := cmd.Flags().Lookup("datadir")
			if datadirFlag != nil {
				datadir := datadirFlag.Value.String()
				if datadir != "" {
					dirPath = filepath.Join(datadir, "logs")
				}
			}
		}
		if logDirPrefix := cmd.Flags().Lookup(LogDirPrefixFlag.Name).Value.String(); len(logDirPrefix) > 0 {
			filePrefix = logDirPrefix
		}
	}

	initSeparatedLogging(log.Root(), filePrefix, dirPath, consoleLevel, dirLevel, consoleJson, dirJson)
	return log.Root()
}

// SetupLoggerCmd performs the logging using parameters specified by the `flag` package and sets it on the root logger
// This is the function which is NOT used by Erigon itself, but instead by utility commands
func SetupLogger(filePrefix string) log.Logger {
	var logConsoleVerbosity = flag.String(LogConsoleVerbosityFlag.Name, "", LogConsoleVerbosityFlag.Usage)
	var logDirVerbosity = flag.String(LogDirVerbosityFlag.Name, "", LogDirVerbosityFlag.Usage)
	var logDirPath = flag.String(LogDirPathFlag.Name, "", LogDirPathFlag.Usage)
	var logDirPrefix = flag.String(LogDirPrefixFlag.Name, "", LogDirPrefixFlag.Usage)
	var logVerbosity = flag.String(LogVerbosityFlag.Name, "", LogVerbosityFlag.Usage)
	var logConsoleJson = flag.Bool(LogConsoleJsonFlag.Name, false, LogConsoleJsonFlag.Usage)
	var logJson = flag.Bool(LogJsonFlag.Name, false, LogJsonFlag.Usage)
	var logDirJson = flag.Bool(LogDirJsonFlag.Name, false, LogDirJsonFlag.Usage)
	flag.Parse()

	var consoleJson = *logJson || *logConsoleJson
	var dirJson = logDirJson

	consoleLevel, lErr := tryGetLogLevel(*logConsoleVerbosity)
	if lErr != nil {
		// try verbosity flag
		consoleLevel, lErr = tryGetLogLevel(*logVerbosity)
		if lErr != nil {
			consoleLevel = log.LvlInfo
		}
	}

	dirLevel, dErr := tryGetLogLevel(*logDirVerbosity)
	if dErr != nil {
		dirLevel = log.LvlInfo
	}

	if logDirPrefix != nil && len(*logDirPrefix) > 0 {
		filePrefix = *logDirPrefix
	}

	initSeparatedLogging(log.Root(), filePrefix, *logDirPath, consoleLevel, dirLevel, consoleJson, *dirJson)
	return log.Root()
}

// initSeparatedLogging construct a log handler accrosing to the configuration parameters passed to it
// and sets the constructed handler to be the handler of the given logger. It then uses that logger
// to report the status of this initialisation
func initSeparatedLogging(
	logger log.Logger,
	filePrefix string,
	dirPath string,
	consoleLevel log.Lvl,
	dirLevel log.Lvl,
	consoleJson bool,
	dirJson bool) {

	var consoleHandler log.Handler

	if consoleJson {
		consoleHandler = log.LvlFilterHandler(consoleLevel, log.StreamHandler(os.Stderr, log.JsonFormat()))
	} else {
		consoleHandler = log.LvlFilterHandler(consoleLevel, log.StderrHandler)
	}
	logger.SetHandler(consoleHandler)

	if len(dirPath) == 0 {
		logger.Info("console logging only")
		return
	}

	// Create the log directory with owner-only permissions (0700) so that
	// by default only the erigon user can read logs. Group or world access
	// must be explicitly granted by the operator (e.g. chmod 750 <logs-dir>).
	// If the directory already exists we inspect its permissions: any group
	// or world access bits are reported as a warning but we proceed running.
	if info, err := os.Stat(dirPath); err != nil {
		if !os.IsNotExist(err) {
			logger.Warn("failed to stat log dir, console logging only", "err", err)
			return
		}
		if err := os.MkdirAll(dirPath, 0700); err != nil {
			logger.Warn("failed to create log dir, console logging only", "err", err)
			return
		}
	} else {
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			logger.Warn("log directory has non-default permissions; group or world access is enabled",
				"dir", dirPath, "mode", fmt.Sprintf("%04o", mode))
		}
	}

	// Pre-create the log file with group-readable permissions (0640).
	// lumberjack.v2 copies the mode of the existing file when rotating, so
	// all subsequent rotated files will also be created with 0640.
	logFilePath := filepath.Join(dirPath, filePrefix+".log")
	if f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640); err == nil {
		_ = f.Close()
	}

	dirFormat := log.TerminalFormatNoColor()
	if dirJson {
		dirFormat = log.JsonFormat()
	}

	lumberjack := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    100, // megabytes
		MaxBackups: 3,
		MaxAge:     28, //days
		Compress:   true,
	}
	userLog := log.StreamHandler(lumberjack, dirFormat)

	mux := log.MultiHandler(consoleHandler, log.LvlFilterHandler(dirLevel, userLog))
	logger.SetHandler(mux)
	logger.Info("logging to file system", "log dir", dirPath, "file prefix", filePrefix, "log level", dirLevel, "json", dirJson)
}

func tryGetLogLevel(s string) (log.Lvl, error) {
	lvl, err := log.LvlFromString(s)
	if err != nil {
		l, err := strconv.Atoi(s)
		if err != nil {
			return 0, err
		}
		return log.Lvl(l), nil
	}
	return lvl, nil
}
