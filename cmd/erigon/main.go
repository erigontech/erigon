package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/pelletier/go-toml"
	"github.com/urfave/cli"
	"gopkg.in/yaml.v2"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/internal/logging"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
)

func main() {
	defer func() {
		panicResult := recover()
		if panicResult == nil {
			return
		}

		log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
		os.Exit(1)
	}()

	app := erigonapp.MakeApp(runErigon, erigoncli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) {
	logger := logging.GetLoggerCtx("erigon", cliCtx)

	// initializing the node and providing the current git commit there
	logger.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	configFilePath := cliCtx.GlobalString(utils.ConfigFlag.Name)
	if configFilePath != "" {
		if err := setFlagsFromConfigFile(cliCtx, configFilePath); err != nil {
			log.Warn("failed setting config flags from yaml/toml file", "err", err)
		}
	}

	nodeCfg := node.NewNodConfigUrfave(cliCtx)
	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg)

	ethNode, err := node.New(nodeCfg, ethCfg, logger)
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return
	}
	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
}

func setFlagsFromConfigFile(ctx *cli.Context, filePath string) error {
	fileExtension := filepath.Ext(filePath)

	fileConfig := make(map[string]interface{})

	if fileExtension == ".yaml" {
		yamlFile, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(yamlFile, fileConfig)
		if err != nil {
			return err
		}
	} else if fileExtension == ".toml" {
		tomlFile, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		err = toml.Unmarshal(tomlFile, &fileConfig)
		if err != nil {
			return err
		}
	} else {
		return errors.New("config files only accepted are .yaml and .toml")
	}
	// sets global flags to value in yaml/toml file
	for key, value := range fileConfig {
		if !ctx.GlobalIsSet(key) {
			if reflect.ValueOf(value).Kind() == reflect.Slice {
				sliceInterface := value.([]interface{})
				s := make([]string, len(sliceInterface))
				for i, v := range sliceInterface {
					s[i] = fmt.Sprintf("%v", v)
				}
				err := ctx.GlobalSet(key, strings.Join(s, ","))
				if err != nil {
					return fmt.Errorf("failed setting %s flag with values=%s error=%s", key, s, err)
				}
			} else {
				err := ctx.GlobalSet(key, fmt.Sprintf("%v", value))
				if err != nil {
					return fmt.Errorf("failed setting %s flag with value=%v error=%s", key, value, err)

				}
			}
		}
	}

	return nil
}
