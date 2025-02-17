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

package util

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

func MakeHttpGetCall(ctx context.Context, url string, data interface{}) error {
	var client = &http.Client{
		Timeout: time.Second * 20,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			return errors.New("it looks like the Erigon node is not running, is running incorrectly, or you have specified the wrong diagnostics URL. If you run the Erigon node with the '--diagnostics.endpoint.addr' or '--diagnostics.endpoint.port' flags, you must also specify the '--debug.addr' flag with the same address and port")
		}
		return err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		if err.Error() == "invalid character 'p' after top-level value" {
			return errors.New("diagnostics was not initialized yet. Please try again in a few seconds")
		}

		return err
	}

	return nil
}

func RenderJson(data interface{}) {
	bytes, err := json.Marshal(data)

	if err == nil {
		fmt.Println(string(bytes))
		fmt.Print("\n")
	}
}

func ExportTable(header table.Row, rows []table.Row, footer table.Row) string {
	if len(rows) > 0 {
		t := CreateTable(header, rows, footer)
		return t.Render()
	}

	return ""
}

func PrintTable(title string, header table.Row, rows []table.Row, footer table.Row) {
	if title != "" {
		txt := text.Colors{text.FgBlue, text.Bold}
		fmt.Println(txt.Sprint(title))

		if len(rows) == 0 {
			txt := text.Colors{text.FgRed, text.Bold}
			fmt.Println(txt.Sprint("No data to show"))
		}
	}

	if len(rows) > 0 {
		t := CreateTable(header, rows, footer)
		t.SetOutputMirror(os.Stdout)
		t.Render()
	}

	fmt.Print("\n")
}

func CreateTable(header table.Row, rows []table.Row, footer table.Row) table.Writer {
	t := table.NewWriter()

	if header != nil {
		t.AppendHeader(header)
	}

	if len(rows) > 0 {
		t.AppendRows(rows)
	}

	if footer != nil {
		t.AppendFooter(footer)
	}

	return t
}

func RenderUseDiagUI() {
	txt := text.Colors{text.BgGreen, text.Bold}
	fmt.Println(txt.Sprint("To get detailed info about Erigon node state use 'diag ui' command."))
}

func RenderError(err error) {
	txt := text.Colors{text.FgWhite, text.BgRed}
	fmt.Printf("%s %s\n", txt.Sprint("[ERROR]"), err)
}

func SaveDataToFile(filePath string, fileName string, data string) error {
	//check is folder exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		err := os.MkdirAll(filePath, 0755)
		if err != nil {
			return err
		}
	}

	fullPath := MakePath(filePath, fileName)

	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(fmt.Sprintf("%v\n", data))
	if err != nil {
		return err
	}

	return nil
}

func MakePath(filePath string, fileName string) string {
	if filePath[len(filePath)-1] == '/' {
		filePath = filePath[:len(filePath)-1]
	}

	return fmt.Sprintf("%s/%s", filePath, fileName)
}
