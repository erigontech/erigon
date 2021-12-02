package cmd

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/starknet/services"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"strings"
)

type Flags struct {
	Contract   string
	PrivateKey string
	Output string
}

var generateRawTxCmd = &cobra.Command{
	Use:   "generateRawTx",
	Short: "Generate data for starknet_sendRawTransaction RPC method",
}

func init() {
	flags := Flags{}

	generateRawTxCmd.Flags().StringVarP(&flags.Contract, "contract", "c", "", "Path to compiled cairo contract in JSON format")
	generateRawTxCmd.MarkFlagRequired("contract")

	generateRawTxCmd.Flags().StringVarP(&flags.PrivateKey, "private_key", "k", "", "Private key")
	generateRawTxCmd.MarkFlagRequired("private_key")

	generateRawTxCmd.Flags().StringVarP(&flags.Output, "output", "o", "", "Path to file where sign transaction will be saved")

	generateRawTxCmd.RunE = func(cmd *cobra.Command, args []string) error {
		rawTxGenerator := services.NewRawTxGenerator(flags.PrivateKey)

		if flags.Output == "" {
			absPath, _ := filepath.Abs(flags.Contract)
			if _, err := os.Stat(absPath); err != nil {
				return fmt.Errorf("could not find contract file: %v", absPath)
			}
		}

		fs := os.DirFS("/")
		buf := bytes.NewBuffer(nil)
		err := rawTxGenerator.CreateFromFS(fs, strings.Trim(flags.Contract, "/"), buf)
		if err != nil {
			return err
		}

		if flags.Output != "" {
			outputFile, err := os.Create(flags.Output)
			if err != nil {
				return fmt.Errorf("could not create output file: %v", flags.Output)
			}
			defer outputFile.Close()

			_, err = outputFile.WriteString(hex.EncodeToString(buf.Bytes()))
		} else {
			fmt.Println(hex.EncodeToString(buf.Bytes()))
		}

		return err
	}

	rootCmd.AddCommand(generateRawTxCmd)
}
