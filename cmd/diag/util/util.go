package util

import (
	"context"
	"encoding/json"
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
			return fmt.Errorf("it looks like the Erigon node is not running, is running incorrectly, or you have specified the wrong diagnostics URL. If you run the Erigon node with the '--diagnostics.endpoint.addr' or '--diagnostics.endpoint.port' flags, you must also specify the '--debug.addr' flag with the same address and port")
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
			return fmt.Errorf("diagnostics was not initialized yet. Please try again in a few seconds")
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

func RenderTableWithHeader(title string, header table.Row, rows []table.Row) {
	if title != "" {
		txt := text.Colors{text.FgBlue, text.Bold}
		fmt.Println(txt.Sprint(title))

		if len(rows) == 0 {
			txt := text.Colors{text.FgRed, text.Bold}
			fmt.Println(txt.Sprint("No data to show"))
		}
	}

	if len(rows) > 0 {
		t := table.NewWriter()
		t.SetOutputMirror(os.Stdout)

		t.AppendHeader(header)
		if len(rows) > 0 {
			t.AppendRows(rows)
		}

		t.AppendSeparator()
		t.Render()
	}

	fmt.Print("\n")
}

func RenderUseDiagUI() {
	txt := text.Colors{text.BgGreen, text.Bold}
	fmt.Println(txt.Sprint("To get detailed info about Erigon node state use 'diag ui' command."))
}

func RenderError(err error) {
	txt := text.Colors{text.FgWhite, text.BgRed}
	fmt.Printf("%s %s\n", txt.Sprint("[ERROR]"), err)
}
