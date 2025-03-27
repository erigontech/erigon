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

package rpctest

import (
	"bufio"
	"fmt"
	"os"
)

func Bench5(erigonURL string) error {

	file, err := os.Open("txs.txt")
	if err != nil {
		panic(err)
	}
	req_id := 0
	template := `{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["0x%s"],"id":%d}`
	var receipt EthReceipt
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		req_id++
		if err = post(client, erigonURL, fmt.Sprintf(template, scanner.Text(), req_id), &receipt); err != nil {
			return fmt.Errorf("Count not get receipt: %s: %v\n", scanner.Text(), err)
		}
		if receipt.Error != nil {
			return fmt.Errorf("Error getting receipt: %d %s\n", receipt.Error.Code, receipt.Error.Message)
		}
	}
	err = scanner.Err()
	if err != nil {
		panic(err)
	}
	return nil
}
