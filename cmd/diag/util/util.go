package util

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
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
		return err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}

	return nil
}

func CalculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "999hrs:99m"
	}
	timeLeftInSeconds := amountLeft / rate

	hours := timeLeftInSeconds / 3600
	minutes := (timeLeftInSeconds / 60) % 60

	return fmt.Sprintf("%dhrs:%dm", hours, minutes)
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
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)

	t.AppendHeader(header)
	if len(rows) > 0 {
		t.AppendRows(rows)
	}

	t.AppendSeparator()
	t.Render()
	fmt.Print("\n")
}

func RenderUseDiagUI() {
	txt := text.Colors{text.BgGreen, text.Bold}
	fmt.Println(txt.Sprint("To get detailed info about Erigon node state use 'diag ui' command."))
}
