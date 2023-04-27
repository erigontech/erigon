package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/log/v3"
	"github.com/pelletier/go-toml"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"

	"github.com/ledgerwatch/erigon/cmd/erigon-el/backend"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/params"
	erigonapp "github.com/ledgerwatch/erigon/turbo/app"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/node"
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

	app := erigonapp.MakeApp("erigon-el", runErigon, erigoncli.DefaultFlags)
	if err := app.Run(os.Args); err != nil {
		_, printErr := fmt.Fprintln(os.Stderr, err)
		if printErr != nil {
			log.Warn("Fprintln error", "err", printErr)
		}
		os.Exit(1)
	}
}

func runErigon(cliCtx *cli.Context) error {
	configFilePath := cliCtx.String(utils.ConfigFlag.Name)
	if configFilePath != "" {
		if err := setFlagsFromConfigFile(cliCtx, configFilePath); err != nil {
			log.Warn("failed setting config flags from yaml/toml file", "err", err)
		}
	}

	logging.SetupLoggerCtx("erigon", cliCtx)

	// initializing the node and providing the current git commit there
	log.Info("Build info", "git_branch", params.GitBranch, "git_tag", params.GitTag, "git_commit", params.GitCommit)

	nodeCfg := node.NewNodConfigUrfave(cliCtx)
	ethCfg := node.NewEthConfigUrfave(cliCtx, nodeCfg)

	ethNode, err := backend.NewNode(nodeCfg, ethCfg, log.Root())
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return err
	}
	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
	return err
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
		if !ctx.IsSet(key) {
			if reflect.ValueOf(value).Kind() == reflect.Slice {
				sliceInterface := value.([]interface{})
				s := make([]string, len(sliceInterface))
				for i, v := range sliceInterface {
					s[i] = fmt.Sprintf("%v", v)
				}
				err := ctx.Set(key, strings.Join(s, ","))
				if err != nil {
					return fmt.Errorf("failed setting %s flag with values=%s error=%s", key, s, err)
				}
			} else {
				err := ctx.Set(key, fmt.Sprintf("%v", value))
				if err != nil {
					return fmt.Errorf("failed setting %s flag with value=%v error=%s", key, value, err)

				}
			}
		}
	}

	return nil
}
