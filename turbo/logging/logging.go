package logging

import (
	"flag"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/urfave/cli/v2"
	"gopkg.in/natefinch/lumberjack.v2"
)

func SetupLoggerCtx(filePrefix string, ctx *cli.Context) {
	var consoleJson = ctx.Bool(LogJsonFlag.Name) || ctx.Bool(LogConsoleJsonFlag.Name)
	var dirJson = ctx.Bool(LogDirJsonFlag.Name)

	consoleLevel, lErr := tryGetLogLevel(ctx.String(LogConsoleVerbosityFlag.Name))
	if lErr != nil {
		// try verbosity flag
		consoleLevel, lErr = tryGetLogLevel(ctx.String(LogVerbosityFlag.Name))
		if lErr != nil {
			consoleLevel = log.LvlInfo
		}
	}

	dirLevel, dErr := tryGetLogLevel(ctx.String(LogDirVerbosityFlag.Name))
	if dErr != nil {
		dirLevel = log.LvlInfo
	}

	dirPath := ctx.String(LogDirPathFlag.Name)
	if dirPath == "" {
		datadir := ctx.String("datadir")
		if datadir != "" {
			dirPath = filepath.Join(datadir, "logs")
		}
	}
	initSeparatedLogging(filePrefix, dirPath, consoleLevel, dirLevel, consoleJson, dirJson)
}

func SetupLoggerCmd(filePrefix string, cmd *cobra.Command) {

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

	dirPath := cmd.Flags().Lookup(LogDirPathFlag.Name).Value.String()
	if dirPath == "" {
		datadir := cmd.Flags().Lookup("datadir").Value.String()
		if datadir != "" {
			dirPath = filepath.Join(datadir, "logs")
		}
	}
	initSeparatedLogging(filePrefix, dirPath, consoleLevel, dirLevel, consoleJson, dirJson)
}

func SetupLogger(filePrefix string) {
	var logConsoleVerbosity = flag.String(LogConsoleVerbosityFlag.Name, "", LogConsoleVerbosityFlag.Usage)
	var logDirVerbosity = flag.String(LogDirVerbosityFlag.Name, "", LogDirVerbosityFlag.Usage)
	var logDirPath = flag.String(LogDirPathFlag.Name, "", LogDirPathFlag.Usage)
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

	initSeparatedLogging(filePrefix, *logDirPath, consoleLevel, dirLevel, consoleJson, *dirJson)
}

func initSeparatedLogging(
	filePrefix string,
	dirPath string,
	consoleLevel log.Lvl,
	dirLevel log.Lvl,
	consoleJson bool,
	dirJson bool) {

	logger := log.Root()

	if consoleJson {
		log.Root().SetHandler(log.LvlFilterHandler(consoleLevel, log.StreamHandler(os.Stderr, log.JsonFormat())))
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(consoleLevel, log.StderrHandler))
	}

	if len(dirPath) == 0 {
		logger.Warn("no log dir set, console logging only")
		return
	}

	err := os.MkdirAll(dirPath, 0764)
	if err != nil {
		logger.Warn("failed to create log dir, console logging only")
		return
	}

	dirFormat := log.TerminalFormatNoColor()
	if dirJson {
		dirFormat = log.JsonFormat()
	}

	lumberjack := &lumberjack.Logger{
		Filename:   path.Join(dirPath, filePrefix+".log"),
		MaxSize:    100, // megabytes
		MaxBackups: 3,
		MaxAge:     28, //days
	}
	userLog := log.StreamHandler(lumberjack, dirFormat)

	mux := log.MultiHandler(logger.GetHandler(), log.LvlFilterHandler(dirLevel, userLog))
	log.Root().SetHandler(mux)
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
