package cmd

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/starknet/services"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

const (
	DefaultGas   = 11_000_000
	DefaultNonce = 0
)

type Flags struct {
	Contract   string
	Salt       string
	Gas        uint64
	Nonce      uint64
	PrivateKey string
	DataDir    string
	Chaindata  string
	Output     string
}

var generateRawTxCmd = &cobra.Command{
	Use:   "generateRawTx",
	Short: "Generate data for starknet_sendRawTransaction RPC method",
}

func init() {
	generateRawTxCmd, flags := config()
	generateRawTxCmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		db, err := db(flags, logger)
		if err != nil {
			return err
		}
		defer db.Close()

		rawTxGenerator := services.NewRawTxGenerator(flags.PrivateKey)

		fs := os.DirFS("/")
		buf := bytes.NewBuffer(nil)

		config := &services.Config{
			ContractFileName: strings.Trim(flags.Contract, "/"),
			Salt:             []byte(flags.Salt),
			Gas:              flags.Gas,
			Nonce:            flags.Nonce,
		}

		err = rawTxGenerator.CreateFromFS(cmd.Context(), fs, db, config, buf)
		if err != nil {
			return err
		}

		if flags.Output != "" {
			outputFile, err := os.Create(flags.Output)
			if err != nil {
				return fmt.Errorf("could not create output file: %v", flags.Output)
			}
			defer outputFile.Close()

			_, err = outputFile.WriteString(buf.String())
			if err != nil {
				return fmt.Errorf("could not write to output file: %v", flags.Output)
			}
		} else {
			fmt.Println(buf.String())
		}

		return err
	}

	rootCmd.AddCommand(generateRawTxCmd)
}

func config() (*cobra.Command, *Flags) {
	flags := &Flags{}
	generateRawTxCmd.PersistentFlags().StringVar(&flags.Contract, "contract", "", "Path to compiled cairo contract in JSON format")
	generateRawTxCmd.MarkPersistentFlagRequired("contract")

	generateRawTxCmd.PersistentFlags().StringVar(&flags.Salt, "salt", "", "Cairo contract address salt")
	generateRawTxCmd.MarkPersistentFlagRequired("salt")

	generateRawTxCmd.PersistentFlags().Uint64Var(&flags.Gas, "gas", DefaultGas, "Gas")

	generateRawTxCmd.PersistentFlags().Uint64Var(&flags.Nonce, "nonce", DefaultNonce, "Nonce")

	generateRawTxCmd.PersistentFlags().StringVar(&flags.PrivateKey, "private_key", "", "Private key")
	generateRawTxCmd.MarkPersistentFlagRequired("private_key")

	generateRawTxCmd.PersistentFlags().StringVar(&flags.DataDir, "datadir", "", "path to Erigon working directory")

	generateRawTxCmd.PersistentFlags().StringVarP(&flags.Output, "output", "o", "", "Path to file where sign transaction will be saved. Print to stdout if empty.")

	generateRawTxCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if flags.DataDir == "" {
			flags.DataDir = paths.DefaultDataDir()
		}
		if flags.Chaindata == "" {
			flags.Chaindata = filepath.Join(flags.DataDir, "chaindata")
		}
		return nil
	}

	return generateRawTxCmd, flags
}

func db(flags *Flags, logger log.Logger) (kv.RoDB, error) {
	rwKv, err := kv2.NewMDBX(logger).Path(flags.Chaindata).Readonly().Open()
	if err != nil {
		return nil, err
	}
	return rwKv, nil
}
