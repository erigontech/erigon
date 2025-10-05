package nodeinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rivo/tview"
	"net/http"
	"time"
)

func Body() (*tview.Flex, *BodyView) {
	view := &BodyView{
		Overview:   tview.NewTextView().SetText("waiting for fetch data from erigon...").SetDynamicColors(true),
		Stages:     tview.NewTextView().SetDynamicColors(true),
		DomainII:   tview.NewTextView().SetDynamicColors(true),
		Clock:      tview.NewTextView().SetTextAlign(tview.AlignRight).SetDynamicColors(true),
		Downloader: tview.NewTextView().SetDynamicColors(true).SetText("loading pls wait [=====------]"),
	}

	topPanel := tview.NewFlex().
		AddItem(view.Overview, 0, 1, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(view.Clock, 1, 1, false).
			AddItem(view.Downloader, 0, 5, false), 0, 1, false)
	topPanel.GetItem(1).(*tview.Flex).GetItem(1).(*tview.TextView).Box.SetBorder(true)
	flex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(topPanel,
			9, 1, false).
		AddItem(tview.NewFlex().
			AddItem(view.Stages, 0, 1, false).
			AddItem(view.DomainII, 0, 1, false),
			0, 1, false)
	flex.Box.SetBorder(true)
	return flex, view
}

type BodyView struct {
	Overview   *tview.TextView
	Stages     *tview.TextView
	DomainII   *tview.TextView
	Clock      *tview.TextView
	Downloader *tview.TextView
}

type TorrentsInfo struct {
	Total    int `json:"total"`
	Complete int `json:"complete"`
}

type DownloaderPinger struct {
	BaseURL string
	Client  *http.Client
}

func NewDownloaderPinger(baseURL string) *DownloaderPinger {
	return &DownloaderPinger{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (d *DownloaderPinger) GetTorrentsInfo(ctx context.Context) (*TorrentsInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.BaseURL+"/downloader/torrentsInfo", nil)
	if err != nil {
		return nil, fmt.Errorf("create request error: %w", err)
	}

	resp, err := d.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var info TorrentsInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode json error: %w", err)
	}

	return &info, nil
}
